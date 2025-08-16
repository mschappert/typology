docker pull mschappert/basegis:latest
docker run --rm -it mschappert/basegis:latest
..............not sure why this doesnt work - look it up

or
#laptop
cd /mnt/d/typology/basegis/
docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis

#desktop
cd /mnt/e/typology/basegis/
docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis