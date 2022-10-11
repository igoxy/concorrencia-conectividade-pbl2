#!/bin/bash

#criar a imagem
sudo docker build -t mosquitto .

#executar o container
#sudo docker container run --rm -it --name mosquitto -p 1884:1884 --net=host mosquitto

#executar sem iteração
sudo docker container run --rm -d --name mosquitto -p 1884:1884 --net=host mosquitto
