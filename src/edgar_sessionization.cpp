// C++17 sessionization engine (portable, robust header parsing)
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

struct Session {
    string ip;
    long long start_ts;
    long long last_ts;
    long long seq;
    int count;
};

static long long to_epoch(const string& date, const string& time) {
    int y=0,m=0,d=0,hh=0,mm=0,ss=0;
    if (sscanf(date.c_str(), "%d-%d-%d", &y,&m,&d) != 3) return -1;
    if (sscanf(time.c_str(), "%d:%d:%d", &hh,&mm,&ss) != 3) return -1;
    int a = (14 - m)/12;
    int y2 = y + 4800 - a;
    int m2 = m + 12*a - 3;
    int JDN = d + (153*m2+2)/5 + 365*y2 + y2/4 - y2/100 + y2/400 - 32045;
    long long days_since_epoch = JDN - 2440588;
    return days_since_epoch*86400LL + hh*3600 + mm*60 + ss;
}

static string from_epoch(long long epoch) {
    time_t tt = (time_t)epoch;
    std::tm t;
    #ifdef _WIN32
    gmtime_s(&t, &tt);
    #else
    gmtime_r(&tt, &t);
    #endif
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             t.tm_year+1900, t.tm_mon+1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
    return string(buf);
}

static vector<string> split_csv_simple(const string& line) {
    vector<string> out; string cur;
    for (char c : line) { if (c==','){ out.push_back(cur); cur.clear(); } else cur.push_back(c); }
    out.push_back(cur); return out;
}

int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " <input/log.csv> <input/inactivity_period.txt> <output/sessionization.txt>\n";
        return 1;
    }
    string log_path = argv[1], inactivity_path = argv[2], out_path = argv[3];

    ifstream finact(inactivity_path);
    if (!finact) { cerr << "Failed to open inactivity_period.txt\n"; return 1; }
    long long inactivity = 0; finact >> inactivity; finact.close();
    if (inactivity < 1 || inactivity > 86400) { cerr << "Invalid inactivity period\n"; return 1; }

    ifstream flog(log_path);
    if (!flog) { cerr << "Failed to open log.csv\n"; return 1; }

    ofstream fout(out_path);
    if (!fout) { cerr << "Failed to open output file\n"; return 1; }

    // Read and parse header
    string header; if (!getline(flog, header)) return 0;
    auto h = split_csv_simple(header);
    // Handle BOM
    if (!h.empty() && h[0].size() >= 3 && (unsigned char)h[0][0] == 0xEF &&
        (unsigned char)h[0][1] == 0xBB && (unsigned char)h[0][2] == 0xBF) {
        h[0] = h[0].substr(3);
    }
    auto trim = [](string& s){
        while(!s.empty() && isspace((unsigned char)s.front())) s.erase(s.begin());
        while(!s.empty() && isspace((unsigned char)s.back())) s.pop_back();
    };
    auto lower = [](string s){ for(char& c: s) c=(char)tolower((unsigned char)c); return s; };

    unordered_map<string,int> idx;
    for (int i=0;i<(int)h.size();++i) {
        trim(h[i]);
        idx[ lower(h[i]) ] = i;
    }
    auto need = [&](initializer_list<string> names)->int{
        for (auto& n : names) { auto it = idx.find(string(n)); if (it!=idx.end()) return it->second; }
        return -1;
    };
    int ip_i   = need({"ip"});
    int date_i = need({"date"});
    int time_i = need({"time"});
    // optional: int cik_i = need({"cik"});
    // optional: int accession_i = need({"accession"});
    int ext_i  = need({"extention","extension","doc"}); // optional for algorithm

    if (ip_i < 0 || date_i < 0 || time_i < 0) {
        cerr << "Missing required columns: need at least ip,date,time\n";
        return 1;
    }

    unordered_map<string, Session> active; active.reserve(1<<16);

    struct Node { long long expiry; long long seq; string ip; };
    struct Cmp { bool operator()(const Node& a, const Node& b) const {
        if (a.expiry != b.expiry) return a.expiry > b.expiry;
        return a.seq > b.seq;
    }};
    priority_queue<Node, vector<Node>, Cmp> pq;

    long long now_ts = LLONG_MIN;
    long long global_seq = 0;

    auto expire_until = [&](long long cutoff_ts) {
        while (!pq.empty()) {
            auto top = pq.top();
            if (top.expiry >= cutoff_ts) break;
            pq.pop();
            auto it = active.find(top.ip);
            if (it == active.end()) continue;
            long long expected = it->second.last_ts + inactivity;
            if (expected != top.expiry) continue; // stale entry
            const Session& s = it->second;
            long long duration = s.last_ts - s.start_ts + 1;
            fout << s.ip << "," << from_epoch(s.start_ts) << "," << from_epoch(s.last_ts)
                 << "," << duration << "," << s.count << "\n";
            active.erase(it);
        }
    };

    string line;
    while (getline(flog, line)) {
        if (line.empty()) continue;
        auto cols = split_csv_simple(line);
        if ((int)cols.size() < (int)h.size()) continue;

        string ip   = cols[ip_i];
        string date = cols[date_i];
        string tm   = cols[time_i];
        long long ts = to_epoch(date, tm);
        if (ts < 0) continue;

        if (now_ts == LLONG_MIN || ts >= now_ts) {
            expire_until(ts);
            now_ts = ts;
        } else {
            now_ts = ts;
        }

        auto it = active.find(ip);
        if (it == active.end()) {
            active.emplace(ip, Session{ip, ts, ts, global_seq++, 1});
        } else {
            it->second.last_ts = ts;
            it->second.count  += 1;
        }
        const Session& sref = active[ip];
        pq.push(Node{ sref.last_ts + inactivity, sref.seq, ip });
    }

    // Flush on EOF: deterministic by start_ts then seq
    vector<Session> rest; rest.reserve(active.size());
    for (auto &kv : active) rest.push_back(kv.second);
    sort(rest.begin(), rest.end(), [](const Session& a, const Session& b){
        if (a.start_ts != b.start_ts) return a.start_ts < b.start_ts;
        return a.seq < b.seq;
    });
    for (const auto& s : rest) {
        long long duration = s.last_ts - s.start_ts + 1;
        fout << s.ip << "," << from_epoch(s.start_ts) << "," << from_epoch(s.last_ts)
             << "," << duration << "," << s.count << "\n";
    }
    return 0;
}
