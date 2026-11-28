package palustre

import "time"

type backoff struct{ first, ceiling time.Duration }

func (p backoff) next(previous time.Duration) time.Duration {
	if previous <= 0 {
		return p.first
	}
	if previous >= p.ceiling/2 {
		return p.ceiling
	}
	return max(p.first, 2*previous)
}

func (p backoff) after(previousFailures int) time.Duration {
	delay := p.first
	for previousFailures > 0 && delay < p.ceiling {
		delay = p.next(delay)
		previousFailures--
	}
	return delay
}

func MembershipRetryDelay(previousFailures int) time.Duration {
	return (backoff{2 * time.Second, time.Hour}).after(previousFailures)
}

func scanPolicy(interval time.Duration) backoff {
	return backoff{time.Minute, max(time.Minute, min(time.Hour, interval))}
}
func ScanRetryDelay(previous, interval time.Duration) time.Duration {
	return scanPolicy(interval).next(previous)
}
func VerificationRetryDelay(previousFailures int) time.Duration {
	return scanPolicy(time.Hour).after(previousFailures)
}

// A connection that stayed up for a minute starts a new retry sequence.
func StreamRetryDelay(previous, connected time.Duration) time.Duration {
	if connected >= time.Minute {
		previous = 0
	}
	return (backoff{time.Second, time.Minute}).next(previous)
}
