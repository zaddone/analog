go build -o analog main.go
go build -o analog_show test.go
scp analog gm@192.168.1.40:~/code/
