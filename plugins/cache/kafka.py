from __future__ import (absolute_import, division, print_function)
from ansible.module_utils._text import to_native
from ansible.utils.display import Display
from ansible.errors import AnsibleError, AnsibleParserError
from ansible import constants as C
from ansible.plugins.callback import CallbackBase
from ansible.parsing.ajson import AnsibleJSONEncoder, AnsibleJSONDecoder
from ansible.plugins.cache import BaseCacheModule
display = Display()
__metaclass__ = type


import os
import json
import functools

DOCUMENTATION = '''
    cache: kafka
    short_description: Use Kafka for writing caching
    description:
        - This cache uses per host records saved in kafka
    version_added: "2.5"
    requirements:
      - kafka
    options:
      _uri:
        description:
          - Not Used see ini settings
        default: 
        required: false
        env:
          - name: ANSIBLE_CACHE_PLUGIN_CONNECTION
        ini:
          - key: fact_caching_connection
            section: defaults
      _prefix:
        description: Not used
        env:
          - name: ANSIBLE_CACHE_PLUGIN_PREFIX
        ini:
          - key: fact_caching_prefix
            section: defaults
      _timeout:
        default: 86400
        description: Not used 
        env:
          - name: ANSIBLE_CACHE_PLUGIN_TIMEOUT
        ini:
          - key: fact_caching_timeout
            section: defaults
        type: integer
'''

class CacheModule(BaseCacheModule):
    def __init__(self,*args, **kwargs):
        #############################################
        ####  Handle default way of configuring plugins even though we won't use it
        #############################################
        try:
          super(CacheModule, self).__init__(*args, **kwargs)
          self._uri     = self.get_option('_uri')
          self._timeout = float(self.get_option('_timeout'))
          self._prefix  = self.get_option('_prefix')
        except KeyError:
          display.deprecated('Rather than importing CacheModules directly, '
                             'use ansible.plugins.loader.cache_loader', version='2.12')
          self._uri     = C.CACHE_PLUGIN_CONNECTION
          self._timeout = float(C.CACHE_PLUGIN_TIMEOUT)
          self._prefix  = C.CACHE_PLUGIN_PREFIX
        ##############################################################
        #### Read default config file
        #### TODO: Add _uri to provide alternate configuration file
        ##############################################################
        #### strip the excption of this file
        cfgFile,ext = os.path.splitext(__file__)
	#### add new INI extention 
        cfgFile    +=".ini"
        display.v("Reading Plugin Config file '%s' " % cfgFile)
        try:
          #### Open the config file
	  fd = open(cfgFile, 'r+')
          #### Read the config file and assign to global varible
	  try:
            cfg = fd.read()
            display.vv("Config Values '%s' " % cfg)
            self._settings = json.loads(cfg)
	  except Exception as e:
            raise AnsibleError('Error Reading config file %s : %s' % cfgFile,to_native(e))
	  finally:
	    fd.close()
        except Exception as e:
	  raise AnsibleError('Error opening %s : %s' % (cfgFile,to_native(e)))
        ####################################################
        #### Initialize Elasticsearch
        ####################################################
        try:
          self._kafka = __import__('KafkaProducer')
        except ImportError:
          raise AnsibleError('Failed to import Kafka module. Maybe you can use pip to install! %s' % to_native(e))
        #####################################################
        #### Initialize _cache file
        #####################################################
        self._cache = {}
        #####################################################
        #### Create connection to the elasticsearch cluster
        #####################################################
        self._producer = self._connect()
        #self._esping()

    #########################################################
    #### Connect to Kafka cluster
    #########################################################
    def _connect(self):
        try:
          return  self._kafka(bootstrap_servers=self._settings['kafka_brokers'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        except Exception as e: 
          raise AnsibleError('Failed to connect to kafka %s %s' % 
				(self._settings['kafka_brokers'], 
				 to_native(e)))
	return

    #########################################################
    ####  Get Cache from local file or Kafka cluster
    #########################################################
    def get(self, key):
        ### DISABLED : TODO
        pass;
        #########################################################
        #### OFFLINE Read from file  
        #########################################################
        #if self._settings['read_local_cache_directory']:
		#cachefile = self._settings['local_cache_directory']+"/"+key 
		#display.v("Skipping elasticserch read")
		#display.v("read_local_cache_directory: reading local file %s" % cachefile)
		#try:
		  #fd = open(cachefile, 'r+')
		  #try:
		    #js = fd.read()
		    #self._cache[key] = json.loads(js)
		  #except Exception as e:
		    #display.error("Error reading cachefile %s : %s" % 
				   #(cachefile,
				    #to_native(e.message)))
		  #finally:
		    #fd.close()
		#except Exception as e:
		  #display.vvv("Error opening cachefile %s " % ( cachefile ))
        #else:
          #######################################################
          #### Read from Kafka cluster
          #########################################################
	  # TODO: Read from kafka

	#return self._cache.get(key)

    #########################################################
    ####  SET Cache  local file and Elasticsearch cluster
    #########################################################
    def set(self, key, value):
       #########################################################
       ####  deep Get Attribute: process  dot notation 
       #########################################################
        def deepgetattr(obj, attr):
          keys = attr.split('.')
          return functools.reduce(lambda d, key: d.get(key) if d else None, keys, obj)

       #########################################################
       ####  deep Set Attribute: process  dot notation 
       #########################################################
        def deepsetattr(attr, val):
          obj={}
          if attr:
            a = attr.pop(0)
            obj[a] = deepsetattr(attr,val)
            return obj
          return val

        #############################################
	#### Unfiltered values json string
        #############################################
	js = json.dumps(value, cls= AnsibleJSONEncoder, sort_keys=True, indent=4)
        #############################################
        ### Write Unfiltered data to local cache file
        #############################################
        if "local_cache_directory" in self._settings:
          #write_local_cache_directory  is assumed not sure why you would turn it off if you set a cache directory
          display.vvv("writing to file %s" % self._settings['local_cache_directory']+"/"+key )
          try:
            ### If path does not exist make it
            if not os.path.exists(self._settings['local_cache_directory']):
                   os.makedirs(self._settings['local_cache_directory'],0755)
            ### Open cache file key should be the hostname from inventory
	    fd = open(self._settings['local_cache_directory']+"/"+key, 'w')
	    ### Write unfiltered data to cache file
	    try:
              fd.write(js)
	    except Exception as e:
	      raise AnsibleError("Error writing date to file  %s with error %s" % 
                         ( self._settings['local_cache_directory']+"/"+key,
                           to_native(e)))
	    finally:
	      fd.close()
	  except Exception as e:
	    raise AnsibleError("Error opening file for writing %s with error %s" % 
                         ( self._settings['local_cache_directory']+"/"+key,
                           to_native(e)))
        else:
            display.vvv("local_cache_directory not set skipping")
	#########################################################
        ### Filter Cache data and send to Kafka
        #########################################################
        # TODO: check if kakfa is available before sending
        if true:
	  filter_val={}
          ### Filter fields
	  for ff in self._settings['field_filter']:
	    attr = ff.split('.')
	    a = attr.pop(0)
	    filter_val[a] = deepsetattr(attr,deepgetattr(value,ff))

	  ### Convert the object json string
	  jd = json.dumps(filter_val, cls=AnsibleJSONEncoder, sort_keys=True, indent=4)
	########################################################
        #%## Send json to Kafka
	########################################################
          try:
            display.vvv("Kafka insert document id='%s' doc = '%s' " % ( value['ansible_hostname'] , jd ))
	    producer.send('test', value )
        # TODO: SEND to Kafka
        #   result = self.es.index(
	#		index=self._settings['es_index'], 
	#		id=value['ansible_hostname'], 
	#		body=jd, doc_type = "_doc" )
        #    if result:
	#      display.vvv("Results %s" % json.dumps(result))
        #      return True
	#    else:
        #      display.error("Results %s" % json.dumps(result))
          except Exception as e:
            raise AnsibleError('Error failed to insert data to kafka %s' % to_native(e))
        return False

    #########################################################
    ####  get the key for all caching objects
    #########################################################
    def keys(self):
	display.v("in keys function %s" % json.dumps(self));
        return self._cache.keys()

    #########################################################
    ####  Is there a Cacheable object available
    #########################################################
    def contains(self, key):
        #####################################################
        #### TODO: Search Kafka
        #####################################################
        containsFileCache = ("local_cache_directory" in self._settings 
                and os.path.exists("%s/%s" % (self._settings['local_cache_directory'],key)))
	display.vvv("contains function return value %s" % containsFileCache )
        return containsFileCache

    #########################################################
    ####  Delete cacheable object
    #########################################################
    def delete(self, key):
        #### Delete from memory cache
        try:
          del self._cache[key]
        except KeyError:
          pass
        ###########################
        #### delete from File cache
        ###########################
        try:
          os.remove("%s/%s" % (self._settings['local_cache_directory'], key))
        except (OSError, IOError):
          pass  
	#TODO Delete from kafka if you can
	#try:
        #  if self._esping():
	#     res = self.es.delete( index=self._settings['es_index'], doc_type="_doc", id=key)
	#     #display.v("display result %s "% to_native(res))
        #     
	#except Exception as e:
	#  raise AnsibleError('Error delete document from elasticsearch %s : %s' % ( key, to_native(e)))
	pass
    #########################################################
    ####  flush cacheable objects: Wipe all cache values
    #########################################################
    def flush(self):
	display.error("flush function not fully implemented");
        #TODO: need to flush from Elasticsearch
        for key in self._cache.keys():
          self.delete(key)
        self._cache = {}

    #########################################################
    ####  copy cacheable objects
    #########################################################
    def copy(self):
        #TODO: need to flush from Elasticsearch
	display.error("copy function not fully implemented");
        ret = dict()
        for key in self.keys():
            ret[key] = self.get(key)
        return ret
