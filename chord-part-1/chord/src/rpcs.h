#ifndef RPCS_H
#define RPCS_H

#include "chord.h"
#include "rpc/client.h"
#include <iostream>
#include <vector>
#include "exception"
#include <unistd.h>

#define m 8 //number of entries in finger table
#define r 3 //number of entries in successor list

int finger_idx = -1;
int next_idx = -1;

Node self, successor, predecessor;

std::vector<Node> finger_table (m, self); //Finger table for the node

std::vector<Node> successor_list(r, self);

Node get_info() { return self; } // Do not modify this line.

Node get_successor() { return successor; }

Node get_predecessor() { return predecessor;}

Node find_successor(uint64_t id); // Function declaration


bool is_between(uint64_t id, uint64_t a, uint64_t b){
  if(a == b){
    return true;
  }
  else if (a < b){
    return (id > a && id <= b);
  } 
  else{     //a > b
    return (id > a || id <= b);
  }
}

void create() {
  predecessor.ip = "";
  successor = self;
}

void join(Node n) {
  predecessor.ip = "";
  rpc::client client(n.ip, n.port);
  successor = client.call("find_successor", self.id).as<Node>();
}

Node closest_preceding_node(uint64_t id){
  for (int i = m -1; i >= 0; i--){
    if (finger_table[i].ip != ""){
      uint64_t finger_id = finger_table[i].id;
      if (is_between(finger_id, self.id, id)){
        return finger_table[i];
      }
    }
  }
  return self;
}

Node find_successor(uint64_t id) {
  if (id == successor.id){
    return successor;
  }
  else if(is_between(id, self.id, successor.id)){
    return successor;
  }
  else{
    Node n = closest_preceding_node(id);
    if(n.id == self.id){
      return successor;
    }
    else{
      rpc::client client(n.ip, n.port);
      return client.call("find_successor", id).as<Node>();
    }
  }
}


void notify(Node n){
  if(predecessor.ip == "" || is_between(n.id, predecessor.id, self.id)){
    predecessor = n;
  }
}

void stabilize(){
  if(successor.ip != ""){
    try{
      rpc::client client(successor.ip, successor.port);
      Node x = client.call("get_predecessor").as<Node>();

      if(x.ip != "" && is_between(x.id, self.id, successor.id)) {
        successor = x;
        rpc::client client2(successor.ip, successor.port);
        client2.call("notify", self);
      }

      if(self.id == successor.id && predecessor.ip != ""){
        successor = predecessor;
      }

      rpc::client client2(successor.ip, successor.port);
      client2.call("notify", self);
    }
    catch(std::exception &e){
      for (int i = 0; i < r; i++){
        try
        {
          rpc::client client3(successor_list[i].ip, successor_list[i].port);
          client3.call("get_info").as<Node>();
          successor = successor_list[i];
          break;
        }
        catch (std::exception &e)
        {
        }
      }
    }
  }
}

void fix_finger(){
  if(successor.ip != "" && predecessor.ip != ""){
    finger_idx = finger_idx + 1;
    if (finger_idx >= m){
      finger_idx = 0;
    }

    uint64_t start = (self.id + (uint64_t)pow(2, 24 + finger_idx)) % (uint64_t)pow(2, 32);
    finger_table[finger_idx] = find_successor(start);
  }
}

void fix_successor(){

  if (successor.ip != ""){
    Node next_node;
    try{
      next_idx = next_idx + 1;
      if (next_idx >= r){
        next_idx = 0;
      }

      if (next_idx == 0){
        successor_list[next_idx] = successor;
      }
      else{
        next_node = successor_list[next_idx - 1];
        try
        {
          rpc::client client3(next_node.ip, next_node.port);
          if (next_node.id == self.id)
          {
            successor_list[next_idx] = successor;
            return;
          }
  
          successor_list[next_idx] = client3.call("get_successor").as<Node>();
          next_node = successor_list[next_idx];
        }
        catch (std::exception &e)
        {
          for (int k = 0; k < r - next_idx; k++){
            successor_list[next_idx - 1 + k] = successor_list[next_idx + k];
          }
          next_idx = - 1;
        }
      }
    }
    catch(std::exception &e){
    }
  }
}

void check_predecessor() {
  try {
    rpc::client client(predecessor.ip, predecessor.port);
    Node n = client.call("get_info").as<Node>();
  } catch (std::exception &e) {
    predecessor.ip = "";
  }
}

void register_rpcs() {
  add_rpc("get_info", &get_info); // Do not modify this line.
  add_rpc("get_successor", &get_successor);
  add_rpc("get_predecessor", &get_predecessor);
  add_rpc("is_between", &is_between);
  add_rpc("create", &create);
  add_rpc("join", &join);
  add_rpc("find_successor", &find_successor);
  add_rpc("notify", &notify);
}

void register_periodics() {
  add_periodic(check_predecessor);
  add_periodic(stabilize);
  add_periodic(fix_finger);
  add_periodic(fix_successor);
}

#endif /* RPCS_H */

