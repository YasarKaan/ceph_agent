import requests
import json
import hashlib
import pika
from datetime import datetime, timedelta
import time
import urllib3
from jinja2 import Environment, FileSystemLoader

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def render_jinja2_template(configs):
    env = Environment(loader=FileSystemLoader(searchpath='./templates'))
    template = env.get_template('ceph_dashboard.j2')
    return json.loads(template.render(configs=configs))


def publish_to_rabbitmq(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='health_diff')
    channel.basic_publish(exchange='', routing_key='health_diff', body=message)
    print(" [x] Sent health diff to RabbitMQ")
    connection.close()


def convert_json(json_data):
    output = {}

    # Health status
    output['health'] = json_data.get("health", {}).get("status", "HEALTH_UNKNOWN")

    # Monitors information
    monitors = json_data.get("mon_status", {}).get("monmap", {}).get("mons", [])
    output['monitors'] = len(monitors)
    output['quorum'] = json_data.get("mon_status", {}).get("quorum", [])

    # Scrub status
    output['scrub_status'] = json_data.get("scrub_status", "Inactive")

    # OSD Status
    osds = json_data.get("osd_map", {}).get("osds", [])
    osd_status = {
        "total": len(osds),
        "up": sum(1 for osd in osds if osd.get("up") == 1),
        "in": sum(1 for osd in osds if osd.get("in") == 1)
    }
    output["osd_status"] = osd_status

    # PG Info
    pg_info = json_data.get("pg_info", {}).get("statuses", {})
    output["pg_info"] = {
        "active_clean": pg_info.get("active+clean", 0)
    }

    # Pools information
    pools = json_data.get("pools", [])
    output['pools'] = len(pools)

    # Disk Usage
    df_stats = json_data.get("df", {}).get("stats", {})
    disk_usage = {
        "total": df_stats.get("total_bytes", 0),
        "used": df_stats.get("total_used_raw_bytes", 0),
        "available": df_stats.get("total_avail_bytes", 0)
    }
    output["disk_usage"] = disk_usage

    # Client Performance
    client_perf = json_data.get("client_perf", {})
    output["client_perf"] = {
        "read_bytes_sec": client_perf.get("read_bytes_sec", 0),
        "write_bytes_sec": client_perf.get("write_bytes_sec", 0),
        "op_per_sec": (
                client_perf.get("read_op_per_sec", 0) +
                client_perf.get("write_op_per_sec", 0)
        )
    }

    # Hosts information
    output["hosts"] = json_data.get("hosts", 0)

    # RGW Count
    output["rgw_count"] = json_data.get("rgw", 0)

    # iSCSI Daemons
    iscsi_daemons = json_data.get("iscsi_daemons", {})
    output["iscsi_daemons"] = {
        "up": iscsi_daemons.get("up", 0),
        "down": iscsi_daemons.get("down", 0)
    }

    return output


def hash_using_sha1(input_text):
    sha1 = hashlib.sha1()
    sha1.update(input_text.encode("utf-8"))
    return sha1.hexdigest()


def authenticate(username, password, url):
    auth_url = f"{url}/api/auth"
    auth_payload = json.dumps({"username": username, "password": password})

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.ceph.api.v1.0+json",
    }

    response = requests.post(auth_url, data=auth_payload, headers=headers, verify=False)
    response.raise_for_status()
    return response.json().get("token")


class CephDashboardService:
    def __init__(self):
        self.last_health_status_hash = None
        self.token = None
        self.token_expiry = datetime.now()

    def get_minimal_health_status(self, username, password, url):
        try:
            self.refresh_token_if_necessary(username, password, url)
            health_status = self.fetch_health_status(url)
            return json.dumps(convert_json(health_status))
        except Exception as e:
            print(f"Error fetching minimal health status: {e}")
            return f"Error fetching minimal health status: {e}"

    def get_health_diff(self, username, password, url):
        try:
            self.refresh_token_if_necessary(username, password, url)
            current_health_status = json.dumps(convert_json(self.fetch_health_status(url)))
            current_health_status_hash = hash_using_sha1(current_health_status)

            if current_health_status_hash != self.last_health_status_hash:
                self.last_health_status_hash = current_health_status_hash
                # publish_to_rabbitmq(current_health_status)
                return print(current_health_status)
            return ""
        except Exception as e:
            return f"Error fetching health status difference: {e}"

    def refresh_token_if_necessary(self, username, password, url):
        if self.token is None or datetime.now() >= self.token_expiry:
            self.token = authenticate(username, password, url)
            self.token_expiry = datetime.now() + timedelta(hours=1)

    def fetch_health_status(self, url):
        health_url = f"{url}/api/health/minimal"
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.ceph.api.v1.0+json"}
        response = requests.get(health_url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()

    def schedule_health_diff_check(self, configs):
        for config in configs:
            username = config["username"]
            password = config["password"]
            url = config["url"]
            interval_seconds = config["interval_seconds"]


            print(f"Starting health diff check for URL: {url}")
            self.get_health_diff(username, password, url)
            time.sleep(interval_seconds)


def main():
    configs = [
        {"username": "admin", "password": "p@ssw0rd", "url": "https://192.168.65.123:8443", "interval_seconds": 15},
        {"username": "admin", "password": "p@ssw0rd", "url": "https://192.178.65.123:8443", "interval_seconds": 15}
    ]

    rendered_configs = render_jinja2_template(configs)

    dashboard_service = CephDashboardService()
    dashboard_service.schedule_health_diff_check(rendered_configs)

if __name__ == "__main__":
    main()

