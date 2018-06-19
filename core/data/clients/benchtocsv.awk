#!/usr/bin/awk -f

/^Benchmark/ {
	name=substr($1, 10)
	split(name, s, "/")
	db=s[1]
	split(s[2], b, "-")
	test=b[1]

	dbs[db]=1
	tests[test]=1
	ops[db ":" test]=$3
}

END {
	printf("\"Test name\"")
	for (d in dbs) {
		printf(",\"%s (ns/op)\"", d)
	}
	printf("\n")

	for (t in tests) {
		printf("\"%s\"", t)
		for (d in dbs) {
			printf(",\"%d\"", ops[d ":" t])
		}	
		printf("\n")
	}
}
