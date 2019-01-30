pkill reads
rm Clusterdb -r
rm nohup.out
go build -o reads read.go
nohup ./reads -n=EUR_JPY &

