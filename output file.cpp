//
//  output file.cpp
//  output file
//
//  Created by Antonio Ache on 4/2/18.
//  Copyright © 2018 Antonio Ache. All rights reserved.
//

#include "output file.hpp"
#include<iostream>
#include<unordered_map>
#include<queue>
#include<vector>
#include<fstream>
#include<sstream>
#include<stack>

using namespace std;

struct Node{ // This is a data type that contains the categories that are going to be displayed in the output file
    std::string ip;// String containing the IP address
    std::time_t start; //start time
    std::time_t end;//ent time
    int count;//this is the number that will determine the order of a session
    int requests;// Number of requests
};

std::time_t convert_to_time(char* reference){ //converts strings representing dete/time to data of type "time"
    struct tm tm;
    strptime(reference,"%D:%H:%M:%S",&tm);
    std::time_t time=mktime(&tm);
    return time;
    
}

char* string_to_char(std::string str){
    std::vector<char> vec(str.begin(),str.end());
    char* output=&vec[0];
    return output;
}

std::string fuse(std::string s1,std::string s2){ //date and time are separate fields, this function "fuses" two strings representing date and time respectively
    std::string::iterator it=s2.begin();
    char c;
    while(it!=s2.end()){
        c=*it;
        s1.push_back(c);
        it++;
    }
    return s1;
}

void write_to_file(std::ofstream &output, Node info){// this method loads the information of a node into the output file
    
    output<<info.ip;
    output<<info.start;
    output<<info.end;
    output<<info.requests;
    //"\n";
}


void process_lines(std::ifstream &file_line,std::unordered_map<std::string,stack<Node>> &IP,double inactivity,int &glob_count)
//This method is the core of the algorithm
//The method consists in filling out a Dictionary or unordered map with the relevant information for every IP address.
//There are two global variables "glob_count" and "inactivity" that are used to organize the sessions in order of appearance.
//For each ip address we consider a node that contains the start time and end time of the session (depending on the inactivity session) and when activity is detected after the inactivity period, we create a new node with the information corresponding to the new session but containing the same IP address. The different nodes corresponding to different sessions for the same IP address will be "stacked up". This is why the main data structure used is a dictionary or unordered map that maps an IP address to a stack of nodes that is created in the same order than the order of creation of the different logs.
{
    std::vector<std::string> row; //vector that contains the information of a particular line
    std::string line;//variable used to feed string streams representing lines in the log file
    std::string ip_address; //string representing the ip from the line
    std::string time_string;// A string representing the time of the request
    char* tmp;
    std::time_t end_time,current_time;
    current_time=std::time(nullptr);
    double difference; //difference of two time variables in seconds
    Node block,top;
    std::stack<Node> stk;
    int req;
    while(std::getline(file_line,line)){
        std::stringstream lineStream(line);
        std::string cell;
        std::string date;
        std::string time;
            while(std::getline(lineStream,cell,',')){
            row.push_back(cell);
            ip_address=row[0];
            date=row[1];
            time=row[2];
            time_string=fuse(date,time);
            if(glob_count==0){
                tmp=string_to_char(time_string);
                current_time=convert_to_time(tmp);//with this we convert a string containing date/time information into a variable of type time_t
            }
                if(IP.count(ip_address)==0){ //If the IP address has not been found yet
                    glob_count++;
                    req=1; //#number of requests is set to 1
                    tmp=string_to_char(time_string);
                    current_time=convert_to_time(tmp);
                    end_time=current_time;
                    block.ip=ip_address;
                    block.start=current_time;
                    block.end=end_time;
                    block.count=glob_count;
                    block.requests=req;
                    stk.push(block);
                    IP.insert(std::pair<std::string,std::stack<Node>>(ip_address,stk));
                    }
                else{
                    top=IP.find(ip_address)->second.top();
                    end_time=top.end;
                    tmp=string_to_char(time_string);
                    current_time=convert_to_time(tmp);
                    difference=difftime(current_time,end_time);
                    if(difference<inactivity){ //If within the inactivity window
                        end_time=current_time; //update time to current_time
                        top.end=end_time;
                        top.requests++;
                        IP.find(ip_address)->second.pop();
                        IP.find(ip_address)->second.push(top);
                    }
                    else{
                        end_time=current_time;  //otherwise create a new session
                        block.start=current_time;
                        block.end=end_time;
                        block.count=glob_count+1;
                        glob_count++;
                        block.requests=1;
                        IP.find(ip_address)->second.push(block);
                        }
                }
                
            
            
        }
        }
    }

int main(int argc,char* arg[]){
    std::string input;
    printf("%s\n","Enter path of test file:");
    std::cin>>input;

    struct global{
        bool operator()(const Node &lhs,const Node &rhs) const{
            return{lhs.count>rhs.count};
        }// we order nodes by global count
    };
    std::priority_queue<Node,std::vector<Node>,global> heap; //we build a heap ordered by global count. The idea of this heap is to produce the desired order in the output.
    std::unordered_map<std::string,std::stack<Node>> IP;
    Node top;
    std::stack<Node> ip_info;
    double inactivity,init;
    for(std::unordered_map<std::string,std::stack<Node>>::iterator it=IP.begin();it!=IP.end();it++){
        ip_info=it->second;
        while(!ip_info.empty()){
            top=ip_info.top();
            heap.push(top);
            ip_info.pop();
        }
    }
    std::ifstream inactivity_file("https://github.com/InsightDataScience/edgar-analytics/tree/master/input/inactivity.txt");
    if (inactivity_file.is_open()){
        std::string line;
        getline(inactivity_file,line);
        std::string::size_type sz;
        init=stod(line,&sz);
    }
    inactivity_file.close();
    inactivity=init;
    std::ofstream output("https://github.com/tonyache/Insight-Coding-Challenge/blob/master/output.txt");
    ifstream main_file(input);
    int glob_count=0;
    if(main_file.is_open()){
       std::string file_row;
       getline(main_file,file_row);
       while(!main_file.eof()){
           getline(main_file,file_row);
           std::stringstream row(file_row);
           process_lines(main_file,IP,inactivity,glob_count);
       }
   output<<"ip_address start_time end_time number_of_documents_requested\n";
    while(!heap.empty()){
        top=heap.top();
        write_to_file(output,top);
        "\n";
    }
    }
    main_file.close();
    
}


