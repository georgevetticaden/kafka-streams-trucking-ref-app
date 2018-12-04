#!/bin/bash

kill $(ps aux | grep 'MicroService' | awk '{print $2}')