from pymongo import MongoClient
from nld_server.server_config import MongoConfigs
from bson.objectid import ObjectId

def generate_uri(user=None, passwd=None):
    if user is not None and passwd is not None:
        return "mongodb://" + user + ':' + passwd + '@' + MongoConfigs.HOST + ':' + str(MongoConfigs.PORT)
    else:
        return "mongodb://" + MongoConfigs.HOST + ":" + str(MongoConfigs.PORT) + "/"


col = {0:'sw_configs_channels', 1:'sw_configs_playlist', 2:'gps_configs'}


class MongoConnection:
    def __init__(self):
        _uri = generate_uri(MongoConfigs.user_id, MongoConfigs.user_passwd)  # for authorized access
        self.client = MongoClient(_uri)

    def specify_by_id(self, id):
        doc = self.client['nld']['user'].find_one(filter={'user_id': id})
        return doc

    def find_by_object_id(self, oid):
        tmp = self.client['nld']['user'].find_one(filter={'_id':oid})
        tmp.pop('_id')
        return tmp

    def return_sw_info(self, user_oid, target):
        target_col = col[target]
        cursor = self.client['nld'][target_col].find(filter={'nld_user_id':user_oid})
        ret = list()
        for it in cursor:
            it.pop('nld_user_id')
            it['_id'] = str(it['_id'])
            ret.append(it)

        return ret

    def return_hw_info(self, user_oid):
        tmp = self.client['nld']['hw_configs'].find_one(filter={'nld_user_id': user_oid})
        tmp.pop('_id')
        tmp.pop('nld_user_id')
        return tmp

    def insert_document(self,user_oid, target, item):
        target_col = col[target]
        dat = dict()
        dat['nld_user_id'] = user_oid
        dat.update(item)
        self.client['nld'][target_col].insert_one(dat)

        return self.return_sw_info(target=target, user_oid=user_oid)

    def modify_document(self, user_oid, modifying_target, target_oid, after_revision):
        if type(target_oid) == str:
            target_oid = ObjectId(target_oid)
        target_col = col[modifying_target]
        self.client['nld'][target_col].find_one_and_update(
            {'_id': target_oid},
            {'$set': after_revision}
        )

        return self.return_sw_info(target=modifying_target, user_oid=user_oid)

    def delete_document(self, user_oid, deleting_target, target_oid):
        if type(target_oid) == str:
            target_oid = ObjectId(target_oid)
        target_col = col[deleting_target]
        self.client['nld'][target_col].delete_one(filter={'_id':target_oid})
        return self.return_sw_info(target=deleting_target, user_oid=user_oid)

    def user_registration(self, user_infos):
        if self.client['nld']['user'].find_one(filter={'user_id' : user_infos['user_id']}) is not None:
            return False
        else:
            record = {'user_id':user_infos['user_id'],
                      'user_passwd': user_infos['passwd'],
                      'user_face_representation' : None,
                      'pin_number' : user_infos['pin_number']
                      }
            uoid = self.client['nld']['user'].insert_one(record)
            self.client['nld']['hw_configs'].insert_one(
                {"nld_user_id" : uoid.inserted_id,
                 "sidemirror_left": 0,
                 "sidemirror_right": 0,
                 "backmirror_angle": 0,
                 "seat_depth": 0,
                 "seat_angle": 0,
                 "handle_height": 0,
                 "moodlight_color": 0}
            )

            return True

    def update_hw_configs(self,user_oid, hw_configs):
        self.client['nld']['hw_configs'].find_one_and_update(
            {'nld_user_id':user_oid},
            {'$set':hw_configs}
        )


    def upload_face_image(self, user_oid, img_representation):
        self.client['nld']['user'].find_one_and_update(
            {'_id': user_oid},
            {'$set':{"user_face_representation" : img_representation}}
        )