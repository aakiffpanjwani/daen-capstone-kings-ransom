import boto3
import datetime
import json
import urllib3
import traceback
import calendar

http = urllib3.PoolManager()

def lambda_handler(event, context):
    try:
        # AWS clients
        s3 = boto3.client("s3", config=boto3.session.Config(connect_timeout=5, read_timeout=10))
        ssm = boto3.client("ssm")

        # Get API key
        parameter_name = "/orderport/api_key"
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        key = response["Parameter"]["Value"]

        # Bucket and log file
        bucket_name = "daen-kings-ransom"
        log_file_key = "logs/LogLastRequested.txt"

        def json_serial(obj):
            if isinstance(obj, datetime.datetime):
                return obj.strftime("%Y-%m-%d")

        # Build API URL
        def api_url(table, clientId, updatedAfterDate=None, updatedBeforeDate=None, orders=False):
            url = f"https://integrations-api.orderport.net/api/Integrations/{table}?clientId={clientId}"
            if orders:
                url += f"&Date={updatedAfterDate}"  # Orders = single day
            else:
                if updatedAfterDate:
                    url += f"&updatedAfter={updatedAfterDate}"
                if updatedBeforeDate:
                    url += f"&updatedBefore={updatedBeforeDate}"
            return url

        # API call
        def api_call(url, key):
            headers = {"x-api-key": str(key), "Content-Type": "application/json"}
            r = http.request("GET", url, headers=headers, timeout=60.0)
            return r

        # Tables and clients
        tables = ["GetCatalog", "GetCustomers", "GetWineClubsAndMembers"]
        orderTable = "GetOrderData"
        clientIds = ["88884419", "88884418"]
        absoluteStartDate = "2024-01-01"

        # Check log file
        log_flag = False
        try:
            response = s3.get_object(Bucket=bucket_name, Key=log_file_key)
            log_contents = response["Body"].read().decode("utf-8").strip()
            log_flag = True
        except s3.exceptions.NoSuchKey:
            log_contents = ""

        today = datetime.date.today()
        updatedBeforeDate = today.strftime("%Y-%m-%d")
        updatedAfterDate = log_contents if log_flag else absoluteStartDate

        start_date = datetime.datetime.strptime(updatedAfterDate, "%Y-%m-%d").date()
        end_date = today

        # --- Non-orders (one snapshot per month) ---
        for table in tables:
            for clientId in clientIds:
                for year in range(start_date.year, end_date.year + 1):
                    for month in range(1, 13):
                        month_start = datetime.date(year, month, 1)
                        if month_start < start_date or month_start > end_date:
                            continue
                        last_day = calendar.monthrange(year, month)[1]
                        month_end = datetime.date(year, month, last_day)

                        url = api_url(table, clientId,
                                      updatedAfterDate=month_start.strftime("%Y-%m-%d"),
                                      updatedBeforeDate=month_end.strftime("%Y-%m-%d"))
                        try:
                            r = api_call(url, key)
                            if r.status == 200:
                                data = json.loads(r.data.decode("utf-8"))
                                month_key = month_start.strftime("%Y-%m-01")
                                json_file_key = f"WineryData/{table}-{clientId}-{month_key}.json"
                                s3.put_object(Bucket=bucket_name, Key=json_file_key,
                                              Body=json.dumps(data, indent=4))
                                print(f"✅ Saved {table} for {clientId} → {json_file_key}")
                            else:
                                print(f"❌ Error {r.status} for {table} {clientId}: {r.data.decode('utf-8')}")
                        except Exception as e:
                            print(f"❌ Error fetching {table} for {clientId}: {e}")
                            traceback.print_exc()

        # --- Orders (grouped monthly) ---
        for clientId in clientIds:
            current_date = start_date
            while current_date <= end_date:
                year, month = current_date.year, current_date.month
                month_start = datetime.date(year, month, 1)
                last_day = calendar.monthrange(year, month)[1]
                month_end = datetime.date(year, month, last_day)

                all_data = []
                d = month_start
                while d <= min(month_end, end_date):
                    A = d.strftime("%m-%d-%Y")
                    url = api_url(orderTable, clientId, A, orders=True)
                    try:
                        r = api_call(url, key)
                        if r.status == 200 and r.data.strip():
                            if b"SqlDateTime overflow" in r.data:
                                print(f"⚠️ SQL error for {A}, skipping.")
                            else:
                                try:
                                    data = json.loads(r.data.decode("utf-8"))
                                    if data:
                                        all_data.append({"date": A, "data": data})
                                        print(f"📦 {len(data)} orders for {clientId} on {A}")
                                except Exception as e:
                                    print(f"❌ JSON error on {A}: {e}")
                                    print(f"Raw: {r.data.decode('utf-8')}")
                        else:
                            print(f"ℹ️ No orders or error for {clientId} on {A}")
                    except Exception as e:
                        print(f"❌ API call failed for {clientId} on {A}: {e}")
                        traceback.print_exc()

                    d += datetime.timedelta(days=1)

                if all_data:
                    month_key = month_start.strftime("%Y-%m-01")
                    json_file_key = f"WineryData/{orderTable}-{clientId}-{month_key}.json"
                    s3.put_object(Bucket=bucket_name, Key=json_file_key,
                                  Body=json.dumps(all_data, indent=4, default=json_serial))
                    print(f"✅ Saved orders for {clientId} → {json_file_key}")

                current_date = month_end + datetime.timedelta(days=1)

        # Update log with last completed month
        last_month_end = datetime.date(end_date.year, end_date.month,
                                       calendar.monthrange(end_date.year, end_date.month)[1])
        s3.put_object(Bucket=bucket_name, Key=log_file_key,
                      Body=last_month_end.strftime("%Y-%m-%d").encode("utf-8"))
        print(f"📝 Log updated to {last_month_end}")

        return {"status": "Operation completed", "api_key_retrieved": True}

    except Exception as e:
        print(f"🔥 Unhandled exception: {e}")
        traceback.print_exc()
        return {"error": str(e)}
