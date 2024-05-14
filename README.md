# One Billion Rows Challenge (Golang)

These are my solutions to the [One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/).
I started with a simple unoptimised solution (solution1.go) which takes 2.24 minutes to an optimised and parallelised solution (solution5.go) which take 6.32 seconds.

**NOTE:** 
* These results were produced on my PC (32GB RAM, 16 core CPU, linux/amd64)
* The major focus was on optimising the solution not necessarily following proper standards.

## USAGE

* Run and benchmark all solutions
```bash
./1brc-go -file=<path_to_weather_data_file>
```

* Run and benchmark a specific solution
```bash
./1brc-go -file=<path_to_weather_data_file> -solution=1
```

* Run CPU profile
```bash
./1brc-go -file=<path_to_weather_data_file> -solution=1 -cpu-profile=cpu.prof
```
