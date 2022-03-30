package s3spanstore

import "time"

func parseDurationWithDefault(stringDuration string, defaultDuration time.Duration) (time.Duration, error) {
	var duration time.Duration
	var err error
	if stringDuration == "" {
		duration = defaultDuration
	} else {
		duration, err = time.ParseDuration(stringDuration)
		if err != nil {
			return duration, err
		}
	}

	return duration, nil
}
