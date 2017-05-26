import socket
import sys
import os
import time
import datetime
import random
import math
import hashlib
import md5
import linecache
from thread import *
from datetime import datetime, timedelta

bs_address = (sys.argv[2],int(sys.argv[4]))
myip = socket.gethostbyname(socket.gethostname())
myport = int(sys.argv[6])
myadd = (myip, myport)
myname = 'KA'
myhash = hashlib.sha1(myip + " " + str(myport))
myhash = myhash.hexdigest()
myhash = myhash[:4]
myid = int(myhash, 16)
print "NODE ID: ", myid
dict = {}
h = []
hsort = []
w, l = 3, 16
finger = [[0 for x in range(w)] for y in range(l)]
log = open('log.txt', 'w')
dlog = open('delay.txt', 'w')
llog = open('latency.txt', 'w')

def reg2bs():													#Registration
	sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
	sock1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock1.connect(bs_address)
	regcmd = "REG" + ' ' + myip + ' '  + str(myport) + ' ' + myname
	reglen = len(regcmd) + 5
	regcmd = "00" + str(reglen) + ' ' + regcmd	
	sock1.sendall(regcmd)
	data = sock1.recv(1024) 
	data = data.split(' ', 1)[1]
	if (data == 'BS REQ -9999'):								#Displaying all the possible error commands
                print 'Unknown command, undefined characters to BS'
        elif (data == 'REGOK KA -1'):
                print 'unknown REG command'
        elif (data == 'REGOK KA 9999'):
                print 'error in registering'
        elif (data == 'REGOK KA 9998'):
                print 'already registered with bs'
        else:
                print 'REGISTERED TO BS'
		log.write("REGISTERED TO BS: " + myip + " at time: " + str(datetime.now()) + "\n")
		splitr = data.split(" ")
		nn = int(splitr[2])
		for i in range (0, nn):
			h.append(hashlib.sha1(splitr[2*i+3] + " " + splitr[2*i+4]))
			h[i] = h[i].hexdigest()
			h[i] = h[i][:4]
			h[i] = int(h[i], 16)
			dict[h[i]] = splitr[2*i+3] + " " + splitr[2*i+4] 					
		ftable()
	sock1.close()
	return

def ftable():													#Build Finger Table
	global s
	global p
	global h4 
	h4 = h[:]
	h4.sort()
	if (len(h) == 0):
		p = myid
		s = myid
	elif (len(h4) > 0):
		s = h4[0]
		p = h4[len(h4)-1]
		for i in range(0, len(h4)-1):
			if ((h4[i] < myid) and (myid < h4[i+1])):
				p = h4[i]
				s = h4[i+1]
				break
		for i in range(0, len(h4)-1):
			if(myid > h4[len(h4)-1] or (myid < h4[0])):
				p = h4[len(h4)-1]
				s = h4[0]
				break
	print "Predecessor: ", p
	print "Successor: ", s
	h5 = h[:]					#: missing
	h5.append(myid)	
	h5.sort()
	for i in range(0, 16):
		finger[i][0] = ((myid + 2**(i)) % 65536)
		finger[i][1] = ((myid + 2**(i+1)) % 65536)
		if (len(h5) == 1):
			finger[i][2] = myid
		else:
			for j in range(0, len(h5)-1):
				if ((finger[i][0] > h5[j]) and (finger[i][0] <= h5[j+1])):
					finger[i][2] = h5[j+1]
					break
				elif(finger[i][0] <= h5[0] or finger[i][0] > h5[len(h5)-1]):
					finger[i][2] = h5[0]
					break
				else:
					abcd = 1
	return

def unreg2bs():														#Unregistration
	sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
	sock2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock2.connect(bs_address)
	unregcmd = "DEL IPADDRESS" + ' ' + myip + ' '  + str(myport) + ' ' + myname
        unreglen = len(unregcmd) + 5
        unregcmd = "00" + str(unreglen) + ' ' + unregcmd
        print 'UNREGISTERING TO BS'
	log.write("UNREGISTERED TO BS: " + myip + " at time: " + str(datetime.now()) + "\n")
        sock2.sendall(unregcmd)
        data = sock2.recv(4096)
        data = data.split(' ', 1)[1]
        if (data == 'BS REQ -9999'):
                print 'Unknown command, undefined characters to BS'
                return
        elif (data == ('DEL IPADDRESS OK -1' or 'DEL OK -1')):
                print 'Error in DEL command'
                return
        elif (data == 'DEL ADDRESS OK ' + myname + ' 9998'):
                print 'This address is not registered'
                return
        else:
                print 'UNREGISTER Successful'
	sock2.close()
        return

def ufg(choice):													#Update Finger Table
	if (choice == '0'):
		ufcmd0 = "UPFIN 0 " + myip + " " + str(myport) + " " + str(myid)
		ufcmd0 = "00" + str(len(ufcmd0)) + " " + ufcmd0
		for key, value in dict.iteritems():
			sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
			sock3.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			splitu = value.split()
			ufo = (splitu[0], int(splitu[1]))
			sock3.connect(ufo)
		        sock3.sendall(ufcmd0)
			data = sock3.recv(4096)
			print "Data on join: ", data
			log.write("JOIN TO DS: " + myip + " at time: " + str(datetime.now()) + "\n")
			sock3.close()
		resources2()		
	elif (choice == '1'):
		ufcmd1 = "UPFIN 1 " + myip + " " + str(myport) + " " + str(myid)
		ufcmd1 = "00" + str(len(ufcmd1)) + " " + ufcmd1
		for key, value in dict.iteritems(): 
			sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
			sock3.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			splitu = value.split()
			ufo = (splitu[0], int(splitu[1]))
			sock3.connect(ufo)
		        sock3.sendall(ufcmd1)
			data = sock3.recv(4096)
			print "Data on leave: ", data
			log.write("LEFT FROM DS: " + myip + " at time: " + str(datetime.now()) + "\n")
			sock3.close()	
	else:
		print "Invalid choice"
	return

def resources2():													#Picking Resources
	target1 = open('r.txt').read().splitlines()
    	target2 = open('entries.txt', 'w')
	target3 = open('key.txt', 'w')
	h3 = h[:]	
	h3.append(myid)
	h3.sort()
    	for i in range(1, 11):
        	myline = random.choice(target1)
		rkey = hashlib.sha1(myline.lower())
		rkey = rkey.hexdigest()
		rkey = rkey[:4]
		rkey = int(rkey, 16) 
		if (len(h3) == 1):
			target3.write(str(rkey) + " " + myline + "\n")
			target2.write(myline + "\n")
		else:
			for i in range(0, len(h3)-1):
				if ((h3[i] < rkey) and (rkey <= h3[i+1])):
					if (h3[i+1] == myid):
						target3.write(str(rkey) + " " + myline + "\n")
						target2.write(myline + "\n")
					else:
						sock6 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
						sock6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
						gkcmd = "GIVEKY 1 " + myip + " " + str(myport) + " " + str(rkey) + " " + myline.replace(" ", "_")
						gkcmd = "00" + str(len(gkcmd)) + " " + gkcmd 
						if (len(h3) == 2):
							brsplit = dict[h[0]]				
						else:
							brsplit = dict[h3[i+1]]	
						splits = brsplit.split()
						sfo = (splits[0], int(splits[1]))
						sock6.connect(sfo)
						sock6.sendall(gkcmd)
						data = sock6.recv(1024)
						log.write("KEY FORWARD: " + str(rkey) + " at time: " + str(datetime.now()) + "\n")
						sock6.close()
					break
				elif(rkey > h3[len(h3)-1] or (rkey <= h3[0])):
					if (h3[0] == myid):
						target3.write(str(rkey) + " " + myline + "\n")
						target2.write(myline + "\n")
					else:
						sock6 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
						sock6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
						gkcmd = "GIVEKY 1 " + myip + " " + str(myport) + " " + str(rkey) + " " + myline.replace(" ", "_")
						gkcmd = "00" + str(len(gkcmd)) + " " + gkcmd 
						brsplit = dict[h3[0]]	
						splits = brsplit.split()
						sfo = (splits[0], int(splits[1]))
						sock6.connect(sfo)
						sock6.sendall(gkcmd)
						data = sock6.recv(1024)
						log.write("KEY FORWARD: " + str(rkey) + " at time: " + str(datetime.now()) + "\n")
						sock6.close()
					break
				else:
					xyz = 2
    	target2.close()
	target3.close()
	duplicate()
	return

def resources():													#Display Resources
	target4 = open('entries.txt', 'r+')
	for line in target4:
		print line
	target4.close()
	return

def keytable():														#Display Key Table
	target5 = open('key.txt', 'r+')
	for line in target5:
		print line
	target5.close()
	return

def addkey():														#Add a key in network
	addf = raw_input("Enter the file name: ")
	target5 = open('entries.txt', 'ab')
	target5.write(addf)
	target5.write('\n')
	target5.close()
	target6 = open('key.txt', 'ab')
	keyn = hashlib.sha1(addf.lower())
	keyn = keyn.hexdigest()
	keyn = keyn[:4]
	keyn = int(keyn, 16)
	h2 = h[:]	
	h2.append(myid)
	h2.sort()
	if (len(h2) == 1):
		target6.write(str(keyn) + " " + addf + "\n")
	else:
		for i in range(0, len(h2)-1):
			if ((h2[i] < keyn) and (keyn <= h2[i+1])):
				if (h2[i+1] == myid):
					target6.write(str(keyn) + " " + addf + "\n")
				else:
					sock6 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
					sock6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					gkcmd = "GIVEKY 1 " + myip + " " + str(myport) + " " + str(keyn) + " " + addf.replace(" ", "_")
					gkcmd = "00" + str(len(gkcmd)) + " " + gkcmd 
					if (len(h2) == 2):
						basplit = dict[h[0]]			
					else:
						basplit = dict[h2[i+1]]	
					splits = basplit.split()
					sfo = (splits[0], int(splits[1]))
					sock6.connect(sfo)
					sock6.sendall(gkcmd)
					data = sock6.recv(1024)
					log.write("KEY FORWARD: " + str(keyn) + " at time: " + str(datetime.now()) + "\n")
					sock6.close()
				break
			elif(keyn > h2[len(h2)-1] or (keyn <= h2[0])):
				if (h2[0] == myid):
					target6.write(str(keyn) + " " + addf + "\n")
				else:
					sock6 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
					sock6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					gkcmd = "GIVEKY 1 " + myip + " " + str(myport) + " " + str(keyn) + " " + addf.replace(" ", "_")
					gkcmd = "00" + str(len(gkcmd)) + " " + gkcmd 
					basplit = dict[h2[0]]	
					splits = basplit.split()
					sfo = (splits[0], int(splits[1]))
					sock6.connect(sfo)
					sock6.sendall(gkcmd)
					data = sock6.recv(1024)
					log.write("KEY FORWARD: " + str(keyn) + " at time: " + str(datetime.now()) + "\n")
					sock6.close()
				break
			else:
				pqr = 3
	target6.close()	
	return

def dispfing():												#Display Finger Table
	print "The current entries in Finger Table are: "
	print "START     RANGE     	   SUCCESSOR"
	for i in range(0, len(finger)):
		print str(finger[i][0]) + "     [" + str(finger[i][0]) + "," + str(finger[i][1]) + ")     " + str(finger[i][2])
	return

def givekey():												#Give Keys to Successor
	target8 = open("key.txt", "r")
	target9 = open("entries.txt", "r")
	kcnt = 0
	gcmd = ""
	for line in target8:
		kcnt = kcnt + 1
		ls = line.split(' ')
		gcmd = gcmd + myip + " " + str(myport) + " " + ls[0] + " "
		if (len(ls) == 2):
			gcmd = gcmd + ls[1]
		elif(len(ls) == 3):
			gcmd = gcmd + ls[1] + "_" + ls[2]
		elif(len(ls) == 4):
			gcmd = gcmd + ls[1] + "_" + ls[2] + "_" + ls[3]
		elif(len(ls) == 5):
			gcmd = gcmd + ls[1] + "_" + ls[2] + "_" + ls[3] + "_" + ls[4]
		elif(len(ls) == 6):
			gcmd = gcmd + ls[1] + "_" + ls[2] + "_" + ls[3] + "_" + ls[4] + "_" + ls[5]
		elif(len(ls) == 7):
			gcmd = gcmd + ls[1] + "_" + ls[2] + "_" + ls[3] + "_" + ls[4] + "_" + ls[5] + "_" + ls[6]
		else:
			tre = 6
	gcmd = "GIVEKY " + str(kcnt) + " " + gcmd
	gcmd = "00" + str(len(gcmd)) + " " + gcmd
	g = gcmd.split('\n')
	p = ""
	for i in range(0, len(g)):
		p = p + g[i] + " "
	sock4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
	sock4.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	gid = dict[s].split()
	gadd = (gid[0], int(gid[1]))
	sock4.connect(gadd)
	sock4.sendall(p)
	log.write("ALL KEYS FORWARDED at time: " + str(datetime.now()) + "\n")
	data = sock4.recv(1024)
	sock4.close()
	open("key.txt", 'w').close()
	open("entries.txt", 'w').close()
	return

def getkey():															#Get Keys from Successor
	sock5 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
	sock5.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	gsid = dict[s].split()
	gsadd = (gsid[0], int(gsid[1]))
	sock5.connect(gsadd)
	gscmd = "GETKY " + str(myid)
	gscmd = "00" + str(len(gscmd)) + " " + gscmd
	sock5.sendall(gscmd)
	data = sock5.recv(1024)
	data1 = data.split()
	target11 = open('key.txt', 'ab')
	if(data[1] == 0):
		return
	for i in range (0, int(data1[2])):
		target11.write(data1[4*i+5] + " " + data1[4*i+6].replace("_"," ") + "\n")
	log.write("KEYS RECEIVED FROM SUCCESSOR at time: " + str(datetime.now()) + "\n")
	target11.close()
	sock5.close()
	return

def search(fin):														#Searching Resources
	lats = time.time()
	fin = fin.lower()
	finh = hashlib.sha1(fin)
	finh = finh.hexdigest()
	finh = finh[:4]
	fid = int(finh, 16)
	tars = open("key.txt", 'r+')
	for line in tars:
		se = line.split()
		if(fid == int(se[0])):
			print "File found at my node only, Hopcount is 0.", line
			log.write("File found at my node at time: " + str(datetime.now()) + ": " + line)
			return
	tars.close()
	secmd = "SER" + " " + myip + " " + str(myport) + " " + str(fid) + " 0"
	secmd = "00" + str(len(secmd)) + " " + secmd
	for i in range (0, len(finger)):
		if((fid >= finger[i][0]) and (fid < finger[i][1])):
			if (finger[i][2] == myid):
				return
			else:
				socks1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
				socks1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				id1 = dict[finger[i][2]].split()
				sfadd1 = (id1[0], int(id1[1]))
				socks1.connect(sfadd1)
				socks1.sendall(secmd)
				log.write("Search sent at time: " + str(datetime.now()) + "\n")
				late = time.time()
				latd = late - lats
				llog.write(str(latd))
				llog.write("\n")
				socks1.close()
				return
		elif(((fid >= finger[i][0]) and (fid > finger[i][1])) or (fid <= finger[i][0]) and (fid < finger[i][1])):
			if (finger[i][0] > finger[i][1]):
				if (finger[i][2] == myid):
					return
				else:
					socks2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
					socks2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					id2 = dict[finger[i][2]].split()
					sfadd2 = (id2[0], int(id2[1]))
					socks2.connect(sfadd2)
					socks2.sendall(secmd)
					log.write("Search sent at time: " + str(datetime.now()) + "\n")
					late = time.time()
					latd = late - lats
					llog.write(str(latd))
					llog.write("\n")
					socks2.close()
					return
		else:
			ytg = 6
			
	return
		
def searchz():												#Zipf Law Implementation	
	tresource = open('r.txt', 'r+')
    	tmed = open('rank.txt', 'w')
    	n = 1
   	for line in tresource:
        	if (n < 10):
            		var = "00" + str(n) + line
            		tmed.write(var)
        	elif ((n<100) and (n>9)):
           		var = "0" + str(n) + line
            		tmed.write(var)
        	else:
            		var = str(n) + line
            		tmed.write(var)
        	n = n+1
    	tresource.close()
   	tmed.close()
															
	s = float(raw_input("Enter s value: "))					
	N = float(raw_input("Enter number of queries to be generated: "))
	trank = open('rank.txt', 'r')
    	tquery = open('queries.txt', 'w')
	a = 0
	sumf = 0.00
	for i in range (1, 109):
		a = a + (1/(math.pow(i, s)))
	for line in trank:
		k = int(line[:3])
		freq = float((1/(math.pow(k, s)))/a)		
		f = (freq*N)
		fround = round(f)
		froundi = int(fround)
		for i in range(1, froundi + 1):
			tquery.write(line[3:])
		sumf += fround
	trank.close()
	tquery.close()
	lines = open('queries.txt').readlines()
	random.shuffle(lines)
	target5 = open('queries.txt', 'rw+').writelines(lines)
	target5 = open('queries.txt', 'r')
	tar = target5.read().splitlines()
	for line in tar:
		search(line)
	return

def thread(connection):									#Multi-threading
    	data = connection.recv(1024)
	std = time.time()
	print "Income data: ", data
	log.write("Data received: " + data + " " + str(datetime.now()) + "\n")
	splitt = data.split()
	if ((splitt[1] == "UPFIN") and (splitt[2] == '0')):
		inip = splitt[3]
		inpo = splitt[4]
		inid = int(splitt[5])
		tcnt = 0
		if (len(h) == 0):
			dict[inid] = inip + " " + inpo
			h.append(inid)
			ftable()
		else:
			for i in range(0, len(h)):
				if (h[i] == inid):
					tcnt = 1
					break
			if (tcnt == 0):						
				dict[inid] = inip + " " + inpo
				h.append(inid)
				ftable()	
		cmd = "0014 UPFINOK 0" 
		connection.sendall(cmd)
		log.write("Finger Table updated on Join at time: " + str(datetime.now()) + "\n")
	elif ((splitt[1] == "UPFIN") and (splitt[2] == '1')):
		inip = splitt[3]
		inpo = splitt[4]
		inid = int(splitt[5])
		if (len(h) == 0):
			print "No nodes to leave"
		else:
			del dict[inid]
			h.remove(inid)
			print "leave h: ", h
			print "leave dict: ", dict
		ftable()	
		cmd = "0014 UPFINOK 0" 
		connection.sendall(cmd)
		log.write("Finger Table updated on Leave at time: " + str(datetime.now()) + "\n")
		print "Node left from the network " + inip + " " + inpo	
	elif(splitt[1] == "GIVEKY"):
		target6 = open('key.txt', 'ab')
		target7 = open('entries.txt', 'ab')
		nkey = int(splitt[2])
		for i in range(0, nkey):
			kip = splitt[4*i+3]
			kpo = splitt[4*i+4]
			kk = splitt[4*i+5]
			kr = splitt[4*i+6]
			kr = kr.replace("_", " ")
			target6.write(kk + " " + kr + "\n")
			target7.write(kr + "\n")
		target6.close()
		target7.close()
		cmd2 = "0015 GIVEKYOK 0" 
		connection.sendall(cmd2)
		log.write("Keys received from Successor at time: " + str(datetime.now()) + "\n")
		duplicate()
	elif(splitt[1] == "GETKY"):	
		pid = int(splitt[2])
		target10 = open('key.txt', 'r+')
		open("mid.txt", 'w').close()
		open("mid1.txt", 'w').close()
		target12 = open('mid.txt', 'ab')
		target16 = open('mid1.txt', 'ab')
		sg = "GETKYOK "
		sgc = 0
		sgi = ""
		for line in target10:
			kc = line.split(' ', 1)
			if ((int(kc[0]) > pid) and (int(kc[0]) <= myid)):
				target12.write(line)
				target16.write(kc[1])
			elif ((int(kc[0]) <= myid) or (int(kc[0]) > pid)):
				if((pid > myid) or (pid < h4[0])):
					target12.write(line)
					target16.write(kc[1])
				else:
					sgi = sgi + myip + " " + str(myport) + " " + kc[0] + " " + kc[1].replace(" ", "_")
					sgc = sgc + 1
			else:
				sgi = sgi + myip + " " + str(myport) + " " + kc[0] + " " + kc[1].replace(" ", "_")
				sgc = sgc + 1
		target10.close()
		target12.close()
		target16.close()	
		target13 = open('mid.txt', 'r+')
		target14 = open("key.txt", 'w')
		target17 = open('mid1.txt', 'r+')
		target18 = open("entries.txt", 'w')
		for line in target13:
			target14.write(line)
		target14.close()
		for line in target17:
			target18.write(line)
		target18.close()
		target13.close()
		target17.close()
		sg = sg + " " + str(sgc) + " " + sgi
		sg = "00" + str(len(sg)) + " " + sg
		connection.sendall(sg)
		log.write("Keys given to Predecessor at time: " + str(datetime.now()) + "\n")
	elif(splitt[1] == "SER"):
		ks = int(splitt[4])
		ksip = splitt[2]
		kspo = int(splitt[3])
		tpo = splitt[3]
		tadd = ksip + " " + tpo
		th = hashlib.sha1(tadd)
		th = th.hexdigest()
		th = th[:4]
		tid = int(th, 16)
		ksadd = (ksip, kspo)
		khop = int(splitt[5])
		ks1 = open("key.txt", 'r+')
		khop = khop + 1
		if (khop>10):
			connection.close()
			log.write("Packet dropped as file not found at time: " + str(datetime.now()) + "\n")
			return
		for line in ks1:
			se = line.split()
			if(ks == int(se[0])):
				print"File found at my node on forward"
				sokm = "SEROK 1 " + myip + " " + str(myport) + " " + line + " " + str(khop)
				sokm = sokm.replace("\n", " ")
				sokm = "00" + str(len(sokm)) + " " + sokm
				sok = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
				sok.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sok.connect(ksadd)
				sok.sendall(sokm)
				etd = time.time()
				delf = etd - std
				dlog.write(str(delf) + "\n")
				log.write("File found and replied: " + str(datetime.now()) + "\n")
				sok.close()
				connection.close()
				return
		ks1.close()
		fm = splitt[0] + " " + splitt[1] + " " + splitt[2] + " " + splitt[3] + " " + splitt[4] + " " + str(int(splitt[5])+1)
		for i in range (0, len(finger)):
			if((ks >= finger[i][0]) and (ks < finger[i][1])):
				if (finger[i][2] == tid):
					connection.close()
					etd = time.time()
					delf = etd - std
					dlog.write(str(delf) + "\n")
					log.write("Packet dropped to avoid loop: " + str(datetime.now()) + "\n")
					return
				else:
					f1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
					f1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					id1 = dict[finger[i][2]].split()
					fadd1 = (id1[0], int(id1[1]))
					f1.connect(fadd1)
					f1.sendall(fm)
					etd = time.time()
					delf = etd - std
					dlog.write(str(delf) + "\n")
					log.write("Request forwarded: " + str(datetime.now()) + "\n")
					f1.close()
					connection.close()
					return
			elif((ks >= finger[i][0]) and (ks > finger[i][1])):
				if (finger[i][0] > finger[i][1]):
					if (finger[i][2] == tid):
						connection.close()
						log.write("Packet dropped to avoid loop: " + str(datetime.now()) + "\n")
						return
					else:
						f2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
						f2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
						id2 = dict[finger[i][2]].split()
						fadd2 = (id2[0], int(id2[1]))
						f2.connect(fadd2)
						f2.sendall(fm)
						etd = time.time()
						delf = etd - std
						dlog.write(str(delf) + "\n")
						log.write("Request forwarded: " + str(datetime.now()) + "\n")
						f2.close()
						connection.close()
						return
			else:
				ytg = 6
	elif(splitt[1] == "TEARDOWN"):
		unreg2bs()
		for i in range(0, len(h)):
			del dict[h[i]]
		print "dictionay: ", dict	
		h10 = h[:]
		for i in range(0, len(h10)):
			h.remove(h10[i])
		for i in range(0, len(finger)):
			finger[i][0] = 0
			finger[i][1] = 0
			finger[i][2] = 0
		print "Tear Down successful"
		log.write("Tear Down successful: " + str(datetime.now()) + "\n")
	
	else:
		efg = 4
	connection.close()
	return
															#Console Thread
def t1(conn):																				
	while True:
		print '0: SETUP NODE, 1: REG, 2: UNREG, 3: UPDATE FINGER, 4: GET KEYS, 5: GIVE KEYS, 6: ADD KEYS, 7: SEARCH, 8: RESOURCES, 9: KEYTABLE, 10: FINGERTABLE, 11. DETAILS, 12. TEARDOWN'
		select = raw_input()
		if (select == "0"):
			reg2bs()
			ufg("0")
			getkey()
		elif (select == "1"):
		        reg2bs()
		elif (select == "2"):
		        unreg2bs()
		elif (select == "3"):
			choice = raw_input ("0: Enter the network, 1: Leave the network \n")
			if (choice == '0'):
				ufg(choice)
			elif(choice == '1'):
				ufg(choice)
				unreg2bs()
				for i in range(0, len(h)):
					del dict[h[i]]
				print "dictionay: ", dict	
				h10 = h[:]
				for i in range(0, len(h10)):
					h.remove(h10[i])
				for i in range(0, len(finger)):
					finger[i][0] = 0
					finger[i][1] = 0
					finger[i][2] = 0
				print "Node leave successful"
				log.write("Node Leave successful: " + str(datetime.now()) + "\n")
			else:
				print "Check argument"
		elif (select == "4"):
		        getkey()
		elif (select == "5"):
		        givekey()
		elif (select == "6"):
			addkey()
		elif (select == "7"):
			mode = raw_input("0: Dynamic, 1: Search through Zipf \n")
			if(mode == '0'):
				fin = raw_input("Enter the filename you want to search \n")
				search(fin)
			elif(mode == '1'):
				searchz()
			else:
				print "check argument"
		elif (select == "8"):
			resources()
		elif (select == "9"):
			keytable()
		elif (select == "10"):
			dispfing()
		elif (select == "11"):
			det = "IP: " + myip + ", Port: " + str(myport) + ", NodeID: " + str(myid)
			print "Details: ", det
		elif (select == "12"):
			for key, value in dict.iteritems(): 
				sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
				sock3.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				tmsg = "0013 TEARDOWN"
				splito = value.split()
				to = (splito[0], int(splito[1]))
				sock3.connect(to)
				sock3.sendall(tmsg)
				data = sock3.recv(4096)
			sock3.close()	
			unreg2bs()
			for i in range(0, len(h)):
				del dict[h[i]]
			print "dictionay: ", dict	
			h10 = h[:]
			for i in range(0, len(h10)):
				h.remove(h10[i])
			for i in range(0, len(finger)):
				finger[i][0] = 0
				finger[i][1] = 0
				finger[i][2] = 0
			print "Tear Down successful"
			log.write("Tear Down successful: " + str(datetime.now()) + "\n")
		else:
			print "check argument"
	return	
	
def duplicate():
	lines = open('key.txt', 'r').readlines()
	lines_set = set(lines)
	out  = open('key.txt', 'w')
	for line in lines_set:
	    	out.write(line)
	lines1 = open('entries.txt', 'r').readlines()
	lines_set1 = set(lines1)
	out  = open('entries.txt', 'w')
	for line in lines_set1:
	    	out.write(line)
	
if __name__=='__main__':											#Main function
	conn = "ac"
	start_new_thread(t1 ,(conn,))	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock.bind(myadd)
	sock.listen(250000)
	try:	
		while True:
        		connection, client_address = sock.accept()
			log.write("Client connected: " + str(client_address) + " " + str(datetime.now()) + "\n")
			start_new_thread(thread ,(connection,))
	except KeyboardInterrupt:		
		print 'Accidental Node Failure'	
		givekey()
		ufg("1")
		unreg2bs()
		for i in range(0, len(h)):
			del dict[h[i]]
		print "dictionay: ", dict	
		h10 = h[:]
		for i in range(0, len(h10)):
			h.remove(h10[i])
		for i in range(0, len(finger)):
			finger[i][0] = 0
			finger[i][1] = 0
			finger[i][2] = 0	
			
	sock.close()	
