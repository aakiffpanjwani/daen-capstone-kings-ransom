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

        # Bucket
        bucket_name = "daen-kings-ransom"

        # JSON serializer
        def json_serial(obj):
            if isinstance(obj, datetime.datetime):
                return obj.strftime("%Y-%m-%d")

        # Build API URL
        def api_url(table, clientId, updatedAfterDate=None, orders=False):
            url = f"https://integrations-api.orderport.net/api/Integrations/{table}?clientId={clientId}"
            if orders:
                url += f"&Date={updatedAfterDate}"  # Orders = single day
            return url

        # API call
        def api_call(url, key):
            headers = {"x-api-key": str(key), "Content-Type": "application/json"}
            return http.request("GET", url, headers=headers, timeout=60.0)

        # Orders only
        orderTable = "GetOrderData"
        clientIds = ["88884419", "88884418"]   # Pearmund, Effingham
        absoluteStartDate = "2024-01-01"

        today = datetime.date.today()

        # --- Orders (monthly aggregation) ---
        for clientId in clientIds:
            log_file_key = f"logs/LogLastRequested-{clientId}.txt"

            # Try to resume from log, else seed
            try:
                response = s3.get_object(Bucket=bucket_name, Key=log_file_key)
                log_contents = response["Body"].read().decode("utf-8").strip()
                start_date = datetime.datetime.strptime(log_contents, "%Y-%m-%d").date()
                print(f"📖 Resuming {clientId} from log date: {start_date}")
            except s3.exceptions.NoSuchKey:
                start_date = datetime.datetime.strptime(absoluteStartDate, "%Y-%m-%d").date()
                s3.put_object(Bucket=bucket_name, Key=log_file_key, Body=absoluteStartDate.encode("utf-8"))
                print(f"🆕 No log found for {clientId}. Starting fresh at {start_date}")

            current_date = start_date

            while current_date <= today:
                # month boundaries
                year, month = current_date.year, current_date.month
                month_start = datetime.date(year, month, 1)
                last_day = calendar.monthrange(year, month)[1]
                month_end = datetime.date(year, month, last_day)

                all_data = []
                crawl_date = month_start

                while crawl_date <= min(month_end, today):
                    A = crawl_date.strftime("%m-%d-%Y")
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

                    crawl_date += datetime.timedelta(days=1)

                # Save monthly file if data exists
                if all_data:
                    month_key = month_start.strftime("%Y-%m-01")
                    json_file_key = f"WineryData/{orderTable}-{clientId}-{month_key}.json"
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=json_file_key,
                        Body=json.dumps(all_data, indent=4, default=json_serial)
                    )
                    print(f"✅ Saved orders for {clientId} → {json_file_key}")

                # Update log after finishing this month
                s3.put_object(
                    Bucket=bucket_name,
                    Key=log_file_key,
                    Body=month_end.strftime("%Y-%m-%d").encode("utf-8")
                )
                print(f"📝 Updated log for {clientId} → {month_end}")

                # Move to next month
                current_date = month_end + datetime.timedelta(days=1)

        return {"status": "Order ingestion completed", "api_key_retrieved": True}

    except Exception as e:
        print(f"🔥 Unhandled exception in lambda_handler: {e}")
        traceback.print_exc()
        return {"error": str(e)}
