# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 03:28:53 2015

@author: Brandon
"""

import zmq
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind('tcp://127.0.0.1:5554')
while True :
    msg = socket.recv()
    socket.send(msg)
