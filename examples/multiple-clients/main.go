package main

import (
	"fmt"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

func main() {
	// create an openGemini client
	configA := &opengemini.Config{
		Addresses: []*opengemini.Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
		CustomMetricsLabels: map[string]string{
			"instance": "client-a",
		},
	}
	clientA, err := opengemini.NewClient(configA)
	if err != nil {
		fmt.Println(err)
		return
	}

	configB := &opengemini.Config{
		Addresses: []*opengemini.Address{{
			Host: "127.0.0.1",
			Port: 8086,
		}},
		CustomMetricsLabels: map[string]string{
			"instance": "client-b",
		},
	}
	clientB, err := opengemini.NewClient(configB)
	if err != nil {
		fmt.Println(err)
		return
	}

	prometheus.MustRegister(clientA.ExposeMetrics(), clientB.ExposeMetrics())

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8089", nil)
}
