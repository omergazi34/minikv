package store

// Match reports whether s matches a Redis-style glob pattern. '*' matches any run of
// characters (including none), '?' exactly one character, '[abc]' one character from a set
// (ranges such as [a-z] and negation [^a] are supported) and a backslash escapes the next
// character.
//
// It works on bytes, iteratively, and backtracks only to the most recent '*', so it runs in
// O(len(pattern) * len(s)) even on adversarial patterns like "*a*a*a*b".
func Match(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, -1
	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if end, ok := matchClass(pattern, p, s[i]); ok {
					p = end
					i++
					continue
				}
			case '\\':
				if p+1 < len(pattern) && pattern[p+1] == s[i] {
					p += 2
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}
		if starP < 0 {
			return false
		}
		// Let the last '*' absorb one more character and retry.
		starI++
		p, i = starP+1, starI
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass tests c against the class starting at pattern[start] == '['. It returns the index
// just past the closing ']' and whether c matched. An unterminated class matches nothing.
func matchClass(pattern string, start int, c byte) (int, bool) {
	p := start + 1
	negate := p < len(pattern) && pattern[p] == '^'
	if negate {
		p++
	}
	matched := false
	for p < len(pattern) && pattern[p] != ']' {
		lo := pattern[p]
		if lo == '\\' && p+1 < len(pattern) {
			p++
			lo = pattern[p]
		}
		if p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']' {
			hi := pattern[p+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if lo <= c && c <= hi {
				matched = true
			}
			p += 3
			continue
		}
		if lo == c {
			matched = true
		}
		p++
	}
	if p >= len(pattern) {
		return 0, false
	}
	return p + 1, matched != negate
}
