package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func ParseTestFile(filename string) ([]TestSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open %s: %v", filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read CSV: %v", err)
	}

	var sets []TestSet
	var cur *TestSet

	for i, rec := range records {
		if i == 0 && len(rec) > 0 && strings.TrimSpace(rec[0]) == "Set Number" {
			continue
		}
		if len(rec) < 2 {
			continue
		}

		setStr := strings.TrimSpace(rec[0])
		txStr := strings.TrimSpace(rec[1])

		if setStr != "" {
			if num, err := strconv.Atoi(setStr); err == nil {
				if cur != nil {
					sets = append(sets, *cur)
				}
				cur = &TestSet{SetNumber: num}
				if len(rec) >= 3 {
					cur.LiveNodes = parseLiveNodes(strings.TrimSpace(rec[2]))
				}
			}
		}

		if cur == nil || txStr == "" {
			continue
		}

		if txStr == "LF" {
			cur.Transactions = append(cur.Transactions, TestTransaction{IsLF: true})
		} else {
			if tx, err := parseTx(txStr); err == nil {
				cur.Transactions = append(cur.Transactions, TestTransaction{Tx: tx})
			}
		}
	}
	if cur != nil {
		sets = append(sets, *cur)
	}
	return sets, nil
}

func parseTx(s string) (Transaction, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	parts := strings.Split(s, ",")
	if len(parts) != 3 {
		return Transaction{}, fmt.Errorf("bad tx: %s", s)
	}
	amt, err := strconv.Atoi(strings.TrimSpace(parts[2]))
	if err != nil {
		return Transaction{}, err
	}
	return Transaction{
		Sender:   strings.TrimSpace(parts[0]),
		Receiver: strings.TrimSpace(parts[1]),
		Amount:   amt,
	}, nil
}

func parseLiveNodes(s string) []int {
	s = strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(s), "]"), "[")
	if s == "" {
		return nil
	}
	var nodes []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(p), "n"))
		if n, err := strconv.Atoi(p); err == nil {
			nodes = append(nodes, n)
		}
	}
	return nodes
}
