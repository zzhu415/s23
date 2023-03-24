import station_pb2, station_pb2_grpc
import grpc
from concurrent import futures
import cassandra
from cassandra.cluster import Cluster, ConsistencyLevel

cluster = Cluster(['p5-db-1', 'p5-db-2', 'p5-db-3'])
cass = cluster.connect()
insert_statement = cass.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?})")
insert_statement.consistency_level = ConsistencyLevel.ONE
max_statement = cass.prepare("SELECT MAX(record.tmax) AS tmax FROM weather.stations WHERE id=?")
max_statement.consistency_level = ConsistencyLevel.THREE

class StationStore(station_pb2_grpc.StationServicer):
    def RecordTemps(self, request, context):
        error = ""
        try:
            cass.execute(insert_statement, (request.station, request.date, request.tmin, request.tmax))
            print("inserted", request.date)
        except ValueError as e:
            print(e, type(e))
            error = str(e)
        return station_pb2.RecordTempsReply(error=error)

    def StationMax(self, request, context):
        tmax = None
        error = ""
        try:
            row = cass.execute(max_statement, (request.station,)).one()
            tmax = row.tmax
            if tmax == None:
                error = "could not compute max"
        except (ValueError, cassandra.Unavailable) as e:
            error = str(e)
        return station_pb2.StationMaxReply(tmax=tmax, error=error)

def serve():
    print("SERVE")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(StationStore(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    print("server started")
    server.wait_for_termination()

serve()
