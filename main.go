package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
)

// List of targets shared across all instances
var targets = []string{
	"https://api.example.com/metrics",
	"https://db1.internal/stats",
	"https://web-01.com/metrics",
	"https://web-02.com/metrics",
	"https://web-03.com/metrics",
	"https://cache-01.internal/metrics",
	"https://cache-02.internal/metrics",
	"https://auth.service.com/metrics",
	"https://payments.internal/metrics",
	"https://queue.internal/metrics",
}

// ---------------------------------------------------------------------
// 1. Thread-Safe Consistent Hash Ring
// ---------------------------------------------------------------------

type HashRing struct {
	mu       sync.RWMutex
	replicas int
	ring     []uint32
	nodes    map[uint32]string
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		nodes:    make(map[uint32]string),
	}
}

func (h *HashRing) Add(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s-%d", node, i)))
		h.ring = append(h.ring, hash)
		h.nodes[hash] = node
	}

	sort.Slice(h.ring, func(i, j int) bool { return h.ring[i] < h.ring[j] })
}

func (h *HashRing) Remove(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s-%d", node, i)))
		delete(h.nodes, hash)

		idx := sort.Search(len(h.ring), func(j int) bool { return h.ring[j] >= hash })
		if idx < len(h.ring) && h.ring[idx] == hash {
			h.ring = append(h.ring[:idx], h.ring[idx+1:]...)
		}
	}
}

func (h *HashRing) GetOwner(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(h.ring), func(i int) bool { return h.ring[i] >= hash })
	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}

// ---------------------------------------------------------------------
// 2. Memberlist Delegate
// ---------------------------------------------------------------------

type eventDelegate struct {
	ring *HashRing
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Printf("[GOSSIP] Node Joined: %s", node.Name)
	e.ring.Add(node.Name)
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Printf("[GOSSIP] Node Left: %s", node.Name)
	e.ring.Remove(node.Name)
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {}

// ---------------------------------------------------------------------
// 3. Main Routine
// ---------------------------------------------------------------------

func main() {
	nodeName := flag.String("name", "", "Unique node identifier")
	port := flag.Int("port", 7946, "Gossip bind port")
	joinAddr := flag.String("join", "", "Existing node address to join (e.g. 127.0.0.1:7946)")
	flag.Parse()

	if *nodeName == "" {
		log.Fatal("Must specify a node name with -name flag")
	}

	ring := NewHashRing(50)
	config := memberlist.DefaultLocalConfig()
	config.Name = *nodeName
	config.BindPort = *port
	config.AdvertisePort = *port
	config.Events = &eventDelegate{ring: ring}

	list, err := memberlist.Create(config)
	if err != nil {
		log.Fatalf("Failed to create memberlist: %v", err)
	}

	if *joinAddr != "" {
		_, err := list.Join([]string{*joinAddr})
		if err != nil {
			log.Fatalf("Failed to join cluster at %s: %v", *joinAddr, err)
		}
	}

	// Mimic scraping work like Grafana/Alloy
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		for range ticker.C {
			var myTargets []string
			for _, target := range targets {
				if ring.GetOwner(target) == *nodeName {
					myTargets = append(myTargets, target)
				}
			}

			log.Printf("== [%s] Active Cluster Nodes: %d | Assigned Targets (%d) ==",
				*nodeName, list.NumMembers(), len(myTargets))
			for _, t := range myTargets {
				log.Printf("[%s] Scraping: %s", *nodeName, t)
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down %s...", *nodeName)
	list.Leave(1 * time.Second)
	_ = list.Shutdown()
}
