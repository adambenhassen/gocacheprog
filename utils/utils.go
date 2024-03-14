package utils

import "time"

func FormatDuration(d time.Duration) string {
	// Start with a scale 100 times greater than a second.
	scale := 100 * time.Second

	// Decrease the scale by a factor of 10 until it's smaller than the duration.
	// This finds the scale that's closest to the duration without exceeding it.
	for scale > d && scale > 100 { // prevent scale from becoming zero
		scale = scale / 10
	}

	// Round the duration to the nearest value in the chosen scale,
	// then convert it to a string and return.
	return d.Round(scale / 100).String()
}
