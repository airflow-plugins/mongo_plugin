import json
import logging

from mongo_plugin.hooks.mongo_hook import MongoHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

from pymongo import ReplaceOne


class S3ToMongoOperator(BaseOperator):
    """
    S3 -> Mongo
    :param s3_conn_id:              The source s3 connnection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param mongo_conn_id:           The destination mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The destination mongo collection.
    :type mongo_collection:         string
    :param mongo_db:                The destination mongo database.
    :type mongo_db:                 string
    :param mongo_method:            The method to push records into mongo. Possible
                                    values for this include:
                                        - insert
                                        - replace
    :type mongo_method:             string
    :param mongo_replacement_filter: *(optional)* If choosing the replace
                                    method, this indicates the the filter to
                                    determine which records to replace. This
                                    may be set as either a string or dictionary.
                                    If set as a string, the operator will
                                    view that as a key and replace the record in
                                    Mongo where the value of the key in the
                                    existing collection matches the incoming
                                    value of the key in the incoming record.
                                    If set as a dictionary, the dictionary will
                                    be passed in normally as a filter. If using
                                    a dictionary and also attempting to filter
                                    based on the value of a certain key
                                    (as in when this is set as a string),
                                    set the key and value as the same.
                                    (e.g. {"customer_id": "customer_id"})
    :type mongo_replacement_filter: string/dictionary
    :param upsert:                  *(optional)* If choosing the replace method,
                                    this is used to indicate whether records should
                                    be upserted.
                                    Defaults to False.
    :type upsert:                   boolean
    """

    template_fields = ('s3_key',
                       'mongo_collection',
                       'mongo_replacement_filter')

    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_method='insert',
                 mongo_db=None,
                 mongo_replacement_filter=None,
                 upsert=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mongo_conn_id = mongo_conn_id
        self.mongo_method = mongo_method
        self.mongo_collection = mongo_collection
        self.mongo_db = mongo_db
        self.mongo_replacement_filter = mongo_replacement_filter
        self.upsert = upsert

        if self.mongo_method not in ('insert', 'replace'):
            raise Exception('Please specify either "insert" or "replace" for Mongo method.')

        if (self.upsert is not True and self.upsert is not False):
            raise Exception('Upsert must be specified as a boolean. Please choose True/False.')

    def execute(self, context):
        s3 = S3Hook(self.s3_conn_id)
        mongo = MongoHook(conn_id=self.mongo_conn_id)

        data = (s3
                .get_key(self.s3_key,
                         bucket_name=self.s3_bucket)
                .get_contents_as_string(encoding='utf-8'))

        docs = [json.loads(doc) for doc in data.split('\n')]

        self.method_mapper(mongo, docs)

    def method_mapper(self, mongo, docs):
        if self.mongo_method == 'insert':
            self.insert_records(mongo, docs)
        else:
            self.replace_records(mongo, docs)

    def insert_records(self, mongo, docs):
        if len(docs) == 1:
            mongo.insert_one(self.mongo_collection,
                             docs[0],
                             mongo_db=self.mongo_db)
        else:
            mongo.insert_many(self.mongo_collection,
                              [doc for doc in docs],
                              mongo_db=self.mongo_db)

    def replace_records(self, mongo, docs):
        operations = []
        for doc in docs:
            mongo_replacement_filter = dict()
            if isinstance(self.mongo_replacement_filter, str):
                mongo_replacement_filter = {self.mongo_replacement_filter: doc.get(self.mongo_replacement_filter, False)}
            elif isinstance(self.mongo_replacement_filter, dict):
                for k, v in self.mongo_replacement_filter.items():
                    if k == v:
                        mongo_replacement_filter[k] = doc.get(k, False)
                    else:
                        mongo_replacement_filter[k] = self.mongo_replacement_filter.get(k, False)

            operations.append(ReplaceOne(mongo_replacement_filter,
                                         doc,
                                         upsert=True))

            # Send once every 1000 in batch
            if (len(operations) == 1000):
                logging.info('Making Request....')
                mongo.bulk_write(self.mongo_collection,
                                 operations,
                                 mongo_db=self.mongo_db,
                                 ordered=False)
                operations = []
                logging.info('Request successfully finished....')

        if (len(operations) > 0):
            logging.info('Making Final Request....')
            mongo.bulk_write(self.mongo_collection,
                             operations,
                             mongo_db=self.mongo_db,
                             ordered=False)
            logging.info('Final Request Finished.')
