#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	cr=openconnection.cursor()
	output_range=list()
	cr.execute("select * from RangeRatingsMetadata;")
	rows=cr.fetchall()
	for row in rows:
		mini=row[1]
		maxi=row[2]
		table="RangeRatingsPart"+str(row[0])
		if not ((ratingMinValue > maxi) or (ratingMaxValue < mini)):
			cr.execute("select * from " + table+ " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
			records = cr.fetchall()			
			for record in records:
				output_range.append([str(table),str(record[0]),str(record[1]),str(record[2])])
			
	cr.execute("select * from RoundRobinRatingsMetadata;")
	rows_rr=cr.fetchall()[0][0]
	for i in range(rows_rr):
		table= "RoundRobinRatingsPart" + str(i)
		cr.execute("select * from " + table + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
		records_rr=cr.fetchall()
	
		for record in records_rr:
				output_range.append([str(table) ,str(record[0]), str(record[1]),str(record[2])])
	writeToFile("RangeQueryOut.txt",output_range)




def PointQuery(ratingsTableName, ratingValue, openconnection):
	cr=openconnection.cursor()
	output_point=list()
	cr.execute("select * from RangeRatingsMetadata;")
	rows=cr.fetchall()
	for row in rows:
		mini=row[1]
		maxi=row[2]
		table="RangeRatingsPart"+str(row[0])
		if ((row[0] == 0 and ratingValue >= mini and ratingValue <= maxi) or (row[0] != 0 and ratingValue > mini and ratingValue <= maxi)):
			cr.execute("select * from " + table + " where rating = " + str(ratingValue) + ";")
			records = cr.fetchall()
			for record in records:
				output_point.append([str(table) , str(record[0]) ,str(record[1]) , str(record[2])])

	cr.execute("select * from RoundRobinRatingsMetadata;")
	rows_rr=cr.fetchall()[0][0]
	for i in range(rows_rr):
		table= "RoundRobinRatingsPart" + str(i)
		cr.execute("select * from " + table + " where rating = " + str(ratingValue) + ";")
		records_rr=cr.fetchall()
		for record in records_rr:
				output_point.append([str(table),str(record[0]),str(record[1]),str(record[2])])
	writeToFile("PointQueryOut.txt",output_point)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
