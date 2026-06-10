package projection

import "strconv"

// xid8 transaction ids round-trip through their decimal TEXT form in this
// package's SQL: the catch-up/lag predicates cast a text bind param to xid8
// ($1::text::xid8) and the SELECT reads transaction_id::text, so the statements
// behave identically under both the simple and extended pgx query protocols
// (xid8's native binary codec is only wired for the extended protocol). These two
// helpers are the single conversion point.

// uint64ToText renders an xid8 transaction id (held as a uint64) as the decimal
// text PostgreSQL parses back into xid8.
func uint64ToText(v uint64) string { return strconv.FormatUint(v, 10) }

// textToUint64 parses the decimal text form of an xid8 transaction id read via
// transaction_id::text back into a uint64.
func textToUint64(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }
