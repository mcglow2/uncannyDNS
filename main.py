import socket
import pyodbc
import os
import configparser
from dnslib.server import DNSServer, DNSLogger, DNSRecord
from dnslib.dns import RR, QTYPE, A, RCODE

config = configparser.ConfigParser()

class AssetsResolver:
    def resolve(self, request, handler):
        matched = False
        reply = request.reply()
        qname = request.q.qname
        qtype = QTYPE[request.q.qtype]

        hostname = request.q.qname.label[0].decode('utf-8')
        dnsname = request.q.qname.label

        # Check for NXDOMAIN
        print("QNAME label= " + str(qname) + "\n")

        if qtype == 'A' and "win.rpi.edu" in str(qname):
            ip = lookup_ip(hostname)
            if ip != '0.0.0.0':
                rr = RR(dnsname, QTYPE.A, rdata=A(ip), ttl=60)

                reply.add_answer(rr)

                print("returned IP %s", (ip))
                return reply

        # Send to to upstream
        upstream = config['DEFAULT']['upstream_ip']
        upstream_port = 53
        if not reply.rr:
            try:
                if handler.protocol == 'udp':
                    proxy_r = request.send(upstream, upstream_port, timeout=60)
                else:
                    proxy_r = request.send(upstream, upstream_port, tcp=True, timeout=60)
                reply = DNSRecord.parse(proxy_r)

            except socket.timeout:
                reply.header.rcode = getattr(RCODE, 'SERVFAIL')

        return reply


def lookup_ip(name):
    server = config['DEFAULT']['server']
    database = config['DEFAULT']['database']
    username = config['DEFAULT']['username']
    password = config['DEFAULT']['password']

    # ENCRYPT=yes;
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';ENCRYPT=no;UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()

    cursor.execute(
        'SELECT AssetName,FQDN,  Userdomain, Username, Description, Lastseen ,tblAssets.AssetID,  IPAddress , DNSName , Serialnumber '+
        ' FROM tblAssets  Inner Join tblAssetCustom On tblAssets.AssetID = tblAssetCustom.AssetID WHERE AssetName = ? And tblAssets.Lastseen > GetDate() - 2; ',
        (name,))

    ip = '0.0.0.0'
    for row in cursor:
        ip = row[7]

    return ip

def run():
    resolver = AssetsResolver()
    logger = DNSLogger(prefix=False)
    server = DNSServer(resolver, port=int(config['DEFAULT']['bind_port']), address=config['DEFAULT']['bind_ip'], logger=logger)
    server.start_thread()

    try:
        while True:
            pass
    except:
        print('exiting')

if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)

    if not os.path.isfile(current_dir + '/config.ini'):
        print("config.ini file is missing")
        exit(1)

    config.read(current_dir + '/config.ini')

    run()