import boto3
from aws_access_key import aws_access_key_ID, aws_secret_access_KEY


class CustomError(Exception):
    def __init__(self, description):
        self.description = description

    def __str__(self):  # overriding print()
        return self.description


class Utility:
    @staticmethod
    def compare_metadata(dict1, dict2):
        for key, val in dict1.items():
            if key not in dict2 or dict2[key] != val:
                return False
        return True

    # list1 = [{'Key': 'tag1', 'Value': 'tag1Val1'}, {'Key': 'tag2', 'Value': 'tag2Val1'}]
    @staticmethod
    def compare_tags(list1, list2):
        for dict1 in list1:
            if dict1 not in list2:
                return False
        return True

    @staticmethod
    def convert_dict_to_str(tags):
        return "&".join([k + "=" + v for k, v in tags.items()])

    @staticmethod
    def take_dict_input(items_cnt):
        dict1 = dict()
        for i in range(items_cnt):
            key_val = input("Enter in format => Key:Value  -> ").split(":")
            dict1[key_val[0]] = key_val[1]
        return dict1

    @staticmethod
    def convert_dict_to_tagset(dict1):
        list1 = []
        for key, val in dict1.items():
            dict2 = dict()
            dict2['Key'] = key
            dict2['Value'] = val
            list1.append(dict2)
        return list1


class S3bucket:
    session = boto3.Session(aws_access_key_id=aws_access_key_ID, aws_secret_access_key=aws_secret_access_KEY,
                            region_name="ap-south-1")
    s3 = session.resource("s3")
    s3_client = session.client("s3")  # created only for accessing object's tags - get_object_tagging()

    def __init__(self, bucket_name):
        try:
            self.bucket = S3bucket.s3.Bucket(bucket_name)
            if bucket_name not in [bucket.name for bucket in
                                   S3bucket.s3.buckets.all()]:  # checking if bucket already exists
                self.bucket.create(CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
        except Exception as e:
            raise CustomError("Error in S3bucket.__init__(): " + str(e))

    def show_objects(self):
        obj_cnt = 0
        try:
            print("Object Key \t Metadata \t Tags")
            for obj_summary in self.bucket.objects.all():
                object = self.bucket.Object(obj_summary.key)
                obj_tags = S3bucket.s3_client.get_object_tagging(Bucket=self.bucket.name, Key=object.key)['TagSet']
                print(object.key, "\t", object.metadata, "\t", obj_tags)
                obj_cnt += 1
            print("Total Objects: ", obj_cnt)
        except Exception as e:
            raise CustomError("Error in S3bucket.__init__(): " + str(e))

    def delete_objects(self, filter_metadata, filter_tags):
        objects_to_delete = []
        try:
            for obj_summary in self.bucket.objects.all():
                object = self.bucket.Object(obj_summary.key)
                obj_tags = S3bucket.s3_client.get_object_tagging(Bucket=self.bucket.name, Key=object.key)['TagSet']
                # print(object.metadata,obj_tags)
                if Utility.compare_metadata(filter_metadata, object.metadata) and Utility.compare_tags(filter_tags,
                                                                                                       obj_tags):
                    objects_to_delete.append({'Key': object.key, 'VersionId': 'null'})
            response = None
            # print(objects_to_delete) -deleting these matched objects
            if objects_to_delete:
                response = self.bucket.delete_objects(Delete={'Objects': objects_to_delete, 'Quiet': True})
            print("Successfully deleted", len(objects_to_delete), "objects!")
            return response
        except Exception as e:
            raise CustomError("Error in S3bucket.delete_objects(): " + str(e))

    def upload_object(self, obj_name, metadata, file_content, tags):
        try:
            response = self.bucket.put_object(Key=obj_name, Body=file_content, Metadata=metadata, Tagging=tags)
            return response
        except Exception as e:
            raise CustomError("Error in S3bucket.upload_object(): " + str(e))


if __name__ == '__main__':
    while True:
        try:
            bucket_name = input("Enter Bucket Name: ")
            # bucket_name='my-new-s3bucket'
            my_bucket = S3bucket(bucket_name)
            while True:
                option = input("\nSelect any option:\n1. Put objects\n2. Display Object details\n3. Delete objects\n4. Exit\n=> ")
                if option == '1':
                    obj_cnt = input("How many objects?")
                    while not obj_cnt.isnumeric():
                        obj_cnt = input("Kindly enter integer! => ")

                    obj_cnt = int(obj_cnt)
                    for i in range(obj_cnt):
                        obj_name = input("Enter Key of the object: ")
                        file_content = ""
                        path = input("Enter path of input file=> ")  # like    C:\Users\Mayura\Downloads\dummy data.txt
                        with open(path, 'rb') as file_obj:
                            file_content = file_obj.read()
                        metadata_cnt = int(input("Count of metadata: "))
                        metadata = Utility.take_dict_input(metadata_cnt)
                        tags_cnt = int(input("Number of Tags: "))
                        tags = Utility.take_dict_input(tags_cnt)
                        my_bucket.upload_object(obj_name, metadata, file_content, Utility.convert_dict_to_str(tags))
                    print("Successfully uploaded", obj_cnt, "objects")
                elif option == '2':
                    my_bucket.show_objects()
                elif option == '3':
                    metadata_cnt = int(input("Count of metadata: "))
                    metadata = Utility.take_dict_input(metadata_cnt)
                    tags_cnt = int(input("Number of Tags: "))
                    tags_set = Utility.convert_dict_to_tagset(Utility.take_dict_input(tags_cnt))
                    response = my_bucket.delete_objects(metadata, tags_set)
                    if response and response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        print("Encountered Error!")
                elif option == '4':
                    break
                else:
                    print("Invalid Option!")
        except Exception as e:
            print("Error: ", e)
        option = input("\nOptions:\n1. Work on another bucket\n2. Exit\n=> ")
        if option == "1":
            continue
        elif option == "2":
            break
        else:
            print("Invalid Option!")
            break
