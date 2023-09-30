# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 13:26:38 2022

@author: Edoardo Campiglio
"""

import sqlite3 as sq
import math as m

file = 'cfv_start.db'

# Creation of orders, planes and flights tables.

with sq.connect(file) as conn:
    c = conn.cursor()
    
    # create table planes_list 
    c.execute("CREATE TABLE planes_list ('id' INTEGER NOT NULL, 'location' TEXT NOT NULL, 'cargo_type' INTEGER NOT NULL, 'status' TEXT, PRIMARY KEY('id' AUTOINCREMENT))")
    
    # cargo types per airport, limits on volumes: small < 500, medium < 1000, large all, heliport < 150 
    small = [1, 2, 6, 7, 10, 12, 13]
    medium = [1, 2, 4, 6, 7, 8, 10, 11, 12, 13]
    large = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    heli = [6, 12]
        
    # list of airports type and iata code
    list_airports=[]
    
    # command to append default
    default = "INSERT INTO planes_list (location, cargo_type, status) VALUES (?, ?, 'available')"
        
    for i in c.execute('SELECT type, iata_code FROM Airports'):       
        list_airports.append(list(i))
    
    # insert planes in airports based on their size
    for i in list_airports:
        if i[0] == 'small_airport':
            for x in small:
                c.execute(default, (i[1], x))
        elif i[0] == 'medium_airport':
            for x in medium:
                c.execute(default, (i[1], x))
        elif i[0] == 'large_airport':
            for x in large:
                c.execute(default, (i[1], x))
        elif i[0] == 'heliport':
            for x in heli:
                c.execute(default, (i[1], x))
    
    

    #create table orders_list
    c.execute("CREATE TABLE orders_list ('id' INTEGER NOT NULL, 'departure' TEXT NOT NULL, \
              'destination' TEXT NOT NULL, 'volume' INTEGER NOT NULL, 'payload' INTEGER NOT NULL,\
              'status' TEXT, 'flight_n' INTEGER, PRIMARY KEY('id' AUTOINCREMENT))")

    #create table flights_list
    c.execute("CREATE TABLE flights_list ('id' INTEGER NOT NULL, 'departure' TEXT NOT NULL,\
              'destination' TEXT NOT NULL, 'plane_code' INTEGER NOT NULL, 'status' TEXT,\
              'volume' INTEGER, 'payload' INTEGER, PRIMARY KEY('id' AUTOINCREMENT))")
    conn.commit()

#     FUNCTION 1/5
def search_for_manifest(flight_number):
    with sq.connect(file) as conn:
        c = conn.cursor()
        orders = []
        for i in c.execute("SELECT id FROM orders_list WHERE flight_n = ?", [flight_number]):
            orders.append(i[0])
            conn.commit()
        if orders == []:
            return print('No flight corresponds to id')
    return orders


#     FUNCTION 2/5
def search_flight_for_route(departure, destination):
    with sq.connect(file) as conn:
        c = conn.cursor()
        
        scheduled = []
        archived = []
        for i in c.execute(
                "SELECT id, status FROM flights_list WHERE \
                    departure = ? AND destination = ?", (departure, destination)
                    ):
            if i[1] == 'scheduled':
                scheduled.append(i[0])
            if i[1] == 'archived':
                archived.append(i[0])
            conn.commit()
        
        return ['SCHEDULED', scheduled, 'ARCHIVED', archived]


#     FUNCTION 3/5
def search_for_unassigned_orders():
    with sq.connect(file) as conn:
        c = conn.cursor()
        orders = []
        for i in c.execute("SELECT id FROM orders_list WHERE status \
                           = 'not assigned'"):
            orders.append(i[0])
        conn.commit()
        return orders
    
    
#     FUNCTION 4/5 
def search_available_planes_for_airport(airport_code):
    with sq.connect(file) as conn:
        c = conn.cursor()
        
        query = "SELECT id FROM planes_list WHERE status = 'available' AND \
            location = ?"
        planes = []
        for i in c.execute(query, [airport_code]):
            planes.append(i[0])
            conn.commit()
       
        return planes


#     FUNCTION 5/5
def load_orders(departure, destination, *orders):
    plane = plane_choice(departure, destination, *orders)
    change_plane_status_to_unavailable(plane)
    flight_generator(departure, destination, plane, *orders)
    change_orders_state(plane, *orders)
    return plane



def add_order_to_dbfile(departure, destination, volume, payload):
    with sq.connect(file) as conn:
        c = conn.cursor()
        
        query = "INSERT INTO orders_list (departure, destination, volume, \
            payload, status) VALUES (?, ?, ?, ?, ?)"
        c.execute(query, (departure, destination
                           , volume, payload, 'not assigned')
                 )
        
        conn.commit() 

#returns distance from coordinates in km    
def distance(departure, destination):
    with sq.connect(file) as conn:
        c = conn.cursor()
        query_lat = "SELECT latitude_deg FROM Airports WHERE iata_code = ?"
        query_lon = "SELECT longitude_deg FROM Airports WHERE iata_code = ?"

        c.execute(query_lat, [departure])
        lat1 = c.fetchone()
        c.execute(query_lat, [destination])
        lat2 = c.fetchone()
        c.execute(query_lon, [departure])
        lon1 = c.fetchone()
        c.execute(query_lon, [destination])
        lon2 = c.fetchone()
        
        unprocessed = [lat1, lat2, lon1, lon2]
        coord = []
        for i in unprocessed:
            coord.append(i[0])
        
        lat1 = m.radians(coord[0])
        lat2 = m.radians(coord[1])
        lon1 = m.radians(coord[2])
        lon2 = m.radians(coord[3])
        
        #Haversine distance formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = m.sin(dlat / 2)**2 + m.cos(lat1) * m.cos(lat2) * m.sin(dlon / 2)**2
        c = 2 * m.asin(m.sqrt(a))
        r = 6371 #in km
        
        conn.commit()

        return (c * r)

#returns the range in km of the cargo type id
def find_range(cargo_type):
    with sq.connect(file) as conn:
        c = conn.cursor()
            
        query = "SELECT Range FROM CargoTypes WHERE CargoTypeId = ?"
        d = []
        for i in c.execute(query, [cargo_type]):
            d.append(i[0])
            conn.commit()
        return d[0]


def sum_volume(*orders):
    with sq.connect(file) as conn:
        c = conn.cursor()
        volume = 0
        query = "SELECT volume FROM orders_list WHERE id = ?"
        
        for order in orders:
            
            for i in c.execute(query, [order]):
                volume += i[0]
                conn.commit()
        return volume


def sum_payload(*orders):
    with sq.connect(file) as conn:
        c = conn.cursor()
        payload = 0
        query = "SELECT payload FROM orders_list WHERE id = ?"
        
        for order in orders:
            
            for i in c.execute(query, [order]):
                payload += i[0]
                conn.commit()
        return payload


#returns list made of [volume, payload] of plane id in cubemeters and kg
def volume_of_cargo(cargo_type):
    with sq.connect(file) as conn:
        c = conn.cursor()
        query = "SELECT volume FROM CargoTypes WHERE CargoTypeId = ?"
        volume = 0
        for i in c.execute(query, [cargo_type]):
            volume = i[0]
            
            conn.commit()
        return volume


#returns list made of [volume, payload] of plane id in cubemeters and kg
def payload_of_cargo(cargo_type):
    with sq.connect(file) as conn:
        c = conn.cursor()
        query = "SELECT payload FROM CargoTypes WHERE CargoTypeId = ?"
        payload = 0
        for i in c.execute(query, [cargo_type]):
            payload = i[0]
            
            conn.commit()
        return payload

#returns from a plane id its cargo type id
def from_plane_to_type(plane_code):
    with sq.connect(file) as conn:
        c = conn.cursor()
        
        query = "SELECT cargo_type FROM planes_list WHERE id = ?"
        code = []
        for i in c.execute(query, [plane_code]):
            code.append(i[0])
            conn.commit()

        return code[0]

#returns planes that have enough capacity and range for orders given
def plane_choice(departure, destination, *orders):
    candidates = search_available_planes_for_airport(departure)
    d = distance(departure, destination)

    result_1 = []
    result_2 = []
    result_final = []
    for i in candidates:
        if find_range(from_plane_to_type(i)) > d:
            result_1.append(i)

    for i in result_1:    
        if volume_of_cargo(from_plane_to_type(i)) > sum_volume(*orders):
            result_2.append(i)
            
    for i in result_2:
        if payload_of_cargo(from_plane_to_type(i)) > sum_payload(*orders):
            result_final.append(i)    
            
    volume = []  
    
    for i in result_final:
        volume.append(volume_of_cargo(from_plane_to_type(i)))
    
    
    
        
    minimumv = min(volume)
    
    choice = 0    
    
    
    for i in result_final:
    
        if volume_of_cargo(from_plane_to_type(i)) == minimumv:
    
            choice = i
        
            break
    
    return choice 


def change_flight_state(flight_id):
    with sq.connect(file) as conn:
        c = conn.cursor()
       
        c.execute("UPDATE flights_list SET status = 'archived' WHERE id = ?", [flight_id])
        
        conn.commit()


def change_plane_status_to_unavailable(plane_code):
        with sq.connect(file) as conn:            
            c = conn.cursor()
            c.execute("UPDATE planes_list SET status = 'unavailable' WHERE id = ?", [plane_code])
            conn.commit()
            
def change_orders_state(plane_code, *orders):
    
        with sq.connect(file) as conn:
            c = conn.cursor()
            for order in orders:
                c.execute("UPDATE orders_list SET status = 'assigned' WHERE id = ?", [order])
                c.execute("UPDATE orders_list SET flight_n = ? WHERE id = ?", (fetch_flight_id_from_plane(plane_code), order))
            conn.commit()        


def fetch_flight_id_from_plane(plane_code):
    with sq.connect(file) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM flights_list WHERE plane_code = ?", [plane_code])
        flight = c.fetchone()[0]
        
        return flight
    
    
def flight_generator(departure, destination, plane_code, *orders):
    with sq.connect(file) as conn:
        c = conn.cursor()
        query = "INSERT INTO flights_list (departure, destination, plane_code, status, volume, payload) VALUES (?, ?, ?, 'scheduled', ?, ?)"
        c.execute(query, (departure, destination, plane_code, sum_volume(*orders), sum_payload(*orders)))
        
        conn.commit()

        
def from_flight_to_plane(flight_id):
    with sq.connect(file) as conn:
        
        c = conn.cursor()
        c.execute("SELECT plane_code FROM flights_list WHERE id = ?", [flight_id])
        plane_code = c.fetchone()[0]
        conn.commit()

        return plane_code

def take_off(flight_id, destination):
    with sq.connect(file) as conn:            
        c = conn.cursor()
        c.execute("UPDATE planes_list SET status = 'available' WHERE \
                  id = ?", [from_flight_to_plane(flight_id)])
        c.execute("UPDATE planes_list SET location = ? WHERE \
                  id = ?", (destination, from_flight_to_plane(flight_id)))
        c.execute("UPDATE flights_list SET status = 'archived' WHERE id = ?", [flight_id])
        conn.commit()
    print("Flight %s has taken off" %(flight_id))
        
        

       
    
    