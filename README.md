## Quick Start
1. Open WSL > use cd to wd
```
cd /mnt/c/Users/username/wherever_repository_is/typology/
```
2. Build Docker Image
```
docker build -t typology_env .
```
3. Run Image
```
  cd /mnt/c/Users/username/wherever_repository_is/typology/
  docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it typology_env
```