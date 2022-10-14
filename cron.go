package worker

import (
	"time"
)

type TimesType int64

const (
	Second TimesType = 1
	Minute TimesType = 60
	Hour   TimesType = Minute * 60
	Day    TimesType = Hour * 24
	Week   TimesType = Day * 7
	Month  TimesType = Day * 31
)

type Times struct {
	Type   TimesType
	Offset int64
	Intv   int64
}

func Secondly() Times {
	return Times{
		Type: Second,
	}
}

func Minutely() Times {
	return Times{
		Type: Minute,
	}
}

func Daily() Times {
	return Times{
		Type: Day,
	}
}

func Weekly() Times {
	return Times{
		Type: Week,
	}
}

func Monthly() Times {
	return Times{
		Type: Month,
	}
}

func (t Times) At(offset time.Duration) Times {
	t.Offset = int64(offset / time.Second)
	if t.Offset > int64(t.Type) {
		t.Offset = 0
	}
	return t
}

func (t Times) Interval(i int64) Times {
	t.Intv = i
	return t
}

func (t Times) Catches(startTime, endTime int64) []time.Time {
	ts := []time.Time{}
	switch t.Type {
	case Second, Minute, Hour, Day:
		intv := int64(t.Type) * t.Intv
		for i := startTime; i < endTime; i++ {
			if (i-t.Offset)%intv == 0 {
				ts = append(ts, time.Unix(i, 0))
			}
		}
	case Week:
		now := time.Unix(startTime, 0)
		_, w := now.ISOWeek()
		if w%int(t.Intv) == 0 {
			
		}
	}
	return ts
}
