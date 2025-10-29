## Quick Start
1. Open WSL > use cd to wd
```
cd /mnt/c/Users/username/wherever_repository_is/basegis/
```
2. Build Docker Image
```
docker build -t basegis .
```
3. Run Image
```
  cd /mnt/c/Users/username/wherever_repository_is/basegis/
  docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis
```

### Notes
docker pull mschappert/basegis:latest
docker run --rm -it mschappert/basegis:latest
....not sure why this doesnt work - look it up

#laptop
cd /mnt/d/typology/
docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis

#desktop
cd /mnt/e/typology/basegis/
docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis

#update docker iamge and push to docker hub
docker build -t basegis .
docker push mschappert/basegis:latest