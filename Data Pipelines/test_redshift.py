import psycopg2

if __name__ == "__main__":
    conn = psycopg2.connect("host= dbname=dev port=5439 user=awsuser password=")
    cur = conn.cursor()
