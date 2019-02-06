pkill reads
rm Clusterdb -r
rm nohup.out reads
go build -o reads read.go
nohup ./reads -n=EUR_JPY &

