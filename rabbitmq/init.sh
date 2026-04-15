#!/bin/bash
sleep 10
rabbitmqctl set_policy DLX ".*" '{"dead-letter-exchange":"dlx"}' --apply-to queues
