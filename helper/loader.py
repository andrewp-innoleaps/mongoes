import json
from pymongo import DESCENDING, UpdateOne
from pymongo.errors import DocumentTooLarge, OperationFailure
from bson import json_util
from bson.objectid import ObjectId
import elasticsearch.helpers

class Loader:
    def __init__(self, ext_con, com_con, settings):
        self.ext_con = ext_con
        self.com_con = com_con
        self.settings = settings
        self.resume_point = None
        self.errors = 0
    
    def log(self, error_type:str, hit) -> None:
        with open(f'errs-{self.errors}.txt', 'a+') as f:
            msg = f'{error_type}:\n hit: {hit}\n'
            f.write(msg)
            self.errors += 1

    def save_data(self, hits, bulk=False):
        if bulk:
            to_save = {}
            for hit in hits:
                if hit['_type'] not in to_save:
                    to_save[hit['_type']] = []
                to_save[hit['_type']].append(hit['_source'])
            for _type, _sources in to_save.items():
                self.com_con['Cursor'][_type].insert_many(_sources, ordered=False)
        else:
            for hit in hits:
                try:
                    self.com_con['Cursor'][hit['_type']].insert_one(hit['_source'])
                except OperationFailure:
                    self.log('OperationFailure', hit['_source'])
                except DocumentTooLarge:
                    self.log('DocumentTooLarge', hit['_source'])

    def transfer_data(self):
        body = {}
        data = self.ext_con['Client'].search(
            index=self.ext_con['Index'],
            scroll=self.settings['SCROLL'],
            size=self.settings['FREQUENCY'],
            body=body
        )

        # Get the scroll ID
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        try:
            iteration = 1
            while scroll_size > 0:
                # Before scroll, process current batch of hits
                print(f"   #{iteration}")
                print(f"   scroll_size:\t\t\t{scroll_size}")
                print(f"   n objects to write:\t\t{len(data['hits']['hits'])}")
                print("=========================================")
                self.save_data(data['hits']['hits'])

                # scroll
                data = self.ext_con['Client'].scroll(scroll_id=sid, scroll=self.settings['SCROLL'])

                # Update the scroll ID
                sid = data['_scroll_id']

                # Get the number of results that returned in the last scroll
                scroll_size = len(data['hits']['hits'])
                iteration += 1
        except Exception as e:
            print("Exception")
            raise e
        finally:
            self.ext_con['Client'].clear_scroll(scroll_id=sid)

    def read_data(self):
        hits = []
        try:
            self.resume_point = self.find_resume_point()
            if 'Index' in self.ext_con:
                body_ = {
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "mongoes_id"
                                    }
                                }
                            }
                        }
                    }
                page_ = self.ext_con['Client'].search(
                            index = self.ext_con['Index'], 
                            size = self.settings['FREQUENCY'], 
                            body=body_
                        )
                hits_ = page_['hits']['hits']
                return hits_
                # return {"status": "succeeded"}
            else:
                pipeline = {
                    "mongoes_id": {
                        "$exists": False
                        }
                    }
                # Mongo Stop Point
                return list(self.ext_con['Collection'].find(pipeline).limit(self.settings['FREQUENCY']))
        except Exception as e:
            # TBD: Handle Exceptions
            hits_ = []
        return hits_

    def tag_documents(self, hits_):
        sour_trace = []
        dest_trace = []

        if 'Index' in self.ext_con:
            for each_hit in hits_:
                dest_trace.append(each_hit['_source'])
                self.resume_point+=1
                # dest_trace[-1]['mongoes_id'] = self.resume_point
                sour_trace.append({
                    "_op_type": "update", 
                    "_index": each_hit["_index"],
                    "_id": each_hit["_id"], 
                    'doc': {
                        "mongoes_id": self.resume_point
                    }
                })
        else:
            for each_hit in hits_:
                each_hit = json.loads(json_util.dumps(each_hit))
                self.resume_point+=1
                # each_hit['mongoes_id'] = self.resume_point
                sour_trace.append({"_id": each_hit["_id"]['$oid'], "mongoes_id": self.resume_point})
                dest_trace.append({
                    '_index': self.com_con['Index'],
                    '_id': self.resume_point,
                    'doc': each_hit
                })
        return sour_trace, dest_trace

    def write_data(self, list_):
        if list_ == []:
            return []
        try:
            ext_trace, com_trace = self.tag_documents(list_)
            if 'Index' in self.ext_con:
                # elasticsearch.helpers.bulk(self.ext_con['Client'], ext_trace)
                if com_trace != []:
                    self.com_con['Collection'].insert_many(com_trace, ordered=False)
                # TBD: Handle successful write
            else:
                bulk = []
                for doc in ext_trace:
                    bulk.append(
                        UpdateOne(
                            {'_id': ObjectId(doc['_id'])},
                            {'$set': {
                                'mongoes_id': doc['mongoes_id']
                                }
                            }
                        )
                    )
                self.ext_con['Collection'].bulk_write(bulk)
                elasticsearch.helpers.bulk(self.com_con['Client'], com_trace)
        except Exception as e:
            ## TBD: Handle Exceptions
            print(e)
            return {}

    def validate_conf(self):
        try:
            self.ext_connection()
        except Exception as e:
            ## TBD: Handle Exceptions in a better manner
            return 'Something is wrong with the connection. Check your connection/config file'

        try:
            self.com_connection()
        except Exception as e:
            ## TBD: Handle Exceptions in a better manner
            return 'Something is wrong with the connection. Check your connection/config file'

        return None
        # TBD: Need to include other test validations right here

    def find_resume_point(self):
        try:
            if 'Index' in self.ext_con:
                # ES's Resume Point
                pipeline = {
                    "aggs" : {
                        "max_mongoes_id" : {
                            "max" : {
                                "field" : "mongoes_id"
                                }
                            }
                        }
                    }
                return self.com_con['Collection'].count_documents({})
                # res_point = self.ext_con['Client'].search(
                #                     index = self.ext_con['Index'], 
                #                     size = 0, 
                #                     body = pipeline)
                # return 0 \
                #     if res_point['aggregations']['max_mongoes_id']['value'] == None \
                #     else int(res_point['aggregations']['max_mongoes_id']['value'])
            else:
                # Mongo's Resume Point
                return list(
                    self.ext_con['Collection']\
                        .find()\
                        .sort("mongoes_id", DESCENDING)\
                        .limit(1))[0]['mongoes_id']
        except Exception as e:
            ## TBD: Handle Exceptions
            return 0

    def find_remaining_count(self):
        try:
            if 'Index' in self.ext_con:
                pipeline = {
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "mongoes_id"
                                }
                            }
                        }
                    }
                }
                remaining_count = self.ext_con['Client'].count(
                    index = self.ext_con['Index'],
                    body = pipeline)
                # ES total
                es_total = int(remaining_count['count'])

                # mongo total
                pipeline = [
                    {"$count": "total_count"},
                ]

                mongo_total = 0 \
                    if self.com_con['Collection'].aggregate(pipeline)._has_next() == False \
                    else self.com_con['Collection'].aggregate(pipeline).next()['total_count']
                return max(0, es_total - mongo_total)
            else:
                pipeline = [{
                        "$match": {
                            "mongoes_id": {
                                "$exists": False
                                }
                            }
                        }, 
                        {
                        "$count": "total_count"
                        }]
                # Mongo Stop Point
                return 0 \
                    if self.ext_con['Collection'].aggregate(pipeline)._has_next() == False \
                    else self.ext_con['Collection'].aggregate(pipeline).next()['total_count']
        except Exception as e:
            print(e)
            ## TBD: Handle Exceptions
            return 0