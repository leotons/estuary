package main

import (
	"sort"
	"time"

	"github.com/filecoin-project/go-address"
)

const minerListTTL = time.Minute

func (cm *ContentManager) sortedMinerList() ([]address.Address, error) {
	cm.minerLk.Lock()
	defer cm.minerLk.Unlock()
	if time.Since(cm.lastComputed) < minerListTTL {
		return cm.sortedMiners, nil
	}

	sml, err := cm.computeSortedMinerList()
	if err != nil {
		return nil, err
	}

	sortedAddrs := make([]address.Address, len(sml))
	for i, m := range sml {
		sortedAddrs[i] = m.Miner
	}

	cm.lastComputed = time.Now()
	cm.sortedMiners = sortedAddrs
	return sortedAddrs, nil
}

type minerDealStats struct {
	Miner address.Address

	TotalDeals     int
	ConfirmedDeals int
	FailedDeals    int
	DealFaults     int
}

func (mds *minerDealStats) SuccessRatio() float64 {
	return float64(mds.ConfirmedDeals) / float64(mds.TotalDeals)
}

// The comparison function that decides 'miner X is better than miner Y'
func (mds *minerDealStats) Better(o *minerDealStats) bool {
	return mds.SuccessRatio() > o.SuccessRatio()
}

func (cm *ContentManager) computeSortedMinerList() ([]*minerDealStats, error) {
	var deals []contentDeal
	if err := cm.DB.Find(&deals).Error; err != nil {
		return nil, err
	}

	stats := make(map[address.Address]*minerDealStats)
	for _, d := range deals {
		maddr, err := d.MinerAddr()
		if err != nil {
			return nil, err
		}

		st, ok := stats[maddr]
		if !ok {
			st = &minerDealStats{
				Miner: maddr,
			}
			stats[maddr] = st
		}

		st.TotalDeals++
		if d.DealID > 0 {
			if d.Failed {
				st.DealFaults++
			} else {
				st.ConfirmedDeals++
			}
		} else if d.Failed {
			st.FailedDeals++
		} else {
			// in progress
		}
	}

	minerStatsArr := make([]*minerDealStats, 0, len(stats))
	for _, st := range stats {
		minerStatsArr = append(minerStatsArr, st)
	}

	sort.Slice(minerStatsArr, func(i, j int) bool {
		return minerStatsArr[i].Better(minerStatsArr[j])
	})

	return minerStatsArr, nil
}