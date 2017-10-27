#!/usr/bin/env python

"""
DOCKER INTERFACE: "the pier"
Adapted from omicron/pier/deploy.py.
Example configurations not included.
"""

__all__ = ['docker','test','docker_list']

import os,sys,re,tempfile,subprocess,shutil,time,json,pwd,datetime,copy,grp
str_types = [str,unicode] if sys.version_info<(3,0) else [str]

#---import for skunkworks
try: from makeface import write_config,read_config
except:
	#---import for factory
	sys.path.insert(0,os.path.join(os.getcwd(),'mill'))
	from config import write_config,read_config

def interpret_docker_instructions(config,mods=None):
	"""
	Read a docker configuration for running things in the docker.
	"""
	if os.path.basename(config)=='config.py':
		raise Exception('you cannot call the config file "config.py" or we have an import failure')
	#---import_remote wraps exec and discards builtins
	from makeface import import_remote
	if not os.path.isfile(config): raise Exception('cannot find %s'%config)
	mod = import_remote(os.path.join('./',config))
	instruct = mod['interpreter'](mods=mods)
	#---validators go here
	return instruct

def docker_list(**kwargs):
	"""
	Summarize the DOCKER.
	"""
	toc_fn = kwargs.pop('toc_fn','docker.json')
	if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
	if not os.path.isfile(toc_fn): raise Exception('[STATUS] missing %s'%toc_fn)
	with open(toc_fn) as fp: toc = json.load(fp)
	import pprint
	pprint.pprint(toc,width=110)

def get_docker_toc(toc_fn):
	"""
	Load any docker table of contents, used to track the history of the dockers.
	!!! Consider just using the configuration for this.
	"""
	toc = {} if not os.path.isfile(toc_fn) else json.load(open(toc_fn))
	return toc

def docker(name,config=None,mods=None,**kwargs):
	"""
	Manage the DOCKER.
	"""
	build_dn = kwargs.pop('build','builds')
	toc_fn = kwargs.pop('toc_fn','docker.json')
	username = kwargs.pop('username','biophyscode')
	config_dict = read_config()
	if config==None: config = config_dict.get('docks_config','docker_config.py')
	if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
	#---get the interpreted docker configuration
	instruct = interpret_docker_instructions(config=config,mods=mods)
	config = config_dict
	#---the name is a sequence
	if name not in instruct.get('sequences',{}): 
		raise Exception('docker configuration lacks a sequence called %s'%name)
	seq = instruct['sequences'][name]
	#---prepare a build directory
	#---! safe to always delete first?
	if os.path.isdir(build_dn): shutil.rmtree(build_dn)
	os.mkdir(build_dn)
	#---get the docker history
	docker_history = config.get('docker_history',{})
	#---process all requirements before making the texts
	reqs = instruct.get('requirements',{})
	for key,val in reqs.items():
		#---perform a simple copy command in docker with a regex substitution for the filename
		if set(val.keys())>=set(['config_keys','filename_sub']): 
			key_path = val['config_keys']
			key_path = tuple([key_path]) if type(key_path) in str_types else key_path
			#---! previously used delve from datapack to do a nested lookup in config, however top-level
			#---! ...keys should be fine and the testset_processor uses a raw lookup of config and 
			#---! ...I would prefer to have the testsets be self-contained.
			if len(key_path)!=1: raise Exception('not ready to do nested lookups. need to add delve back.')
			if key_path[0] not in config:
				raise Exception('failed to get item %s from the config dictionary.'%str(key_path)+msg)
			spot = config[key_path[0]]
			#---substitute in the dockerfile
			if key not in instruct['dockerfiles']: raise Exception('cannot find %s in dockerfiles'%key)
			instruct['dockerfiles'][key] = re.sub(val['filename_sub'],
				os.path.basename(spot),instruct['dockerfiles'][key])
			for sub_from,sub_to in val.get('subs',{}).items():
				instruct['dockerfiles'][key] = re.sub(sub_from,sub_to,instruct['dockerfiles'][key])
			#---we always have to copy the file to the docker build directory
			shutil.copyfile(spot,os.path.join(build_dn,os.path.basename(spot)))
		else: raise Exception('cannot get requirement for %s: %s'%(key,val))
	#---prepare the texts of the dockerfiles
	steps = seq.split()
	texts = [(step,instruct['dockerfiles'][step]) for step in steps]
	#---final stage adds the user
	this_user = pwd.getpwnam(os.environ['USER'])
	this_user_details = {'gid':this_user.pw_gid,'uid':this_user.pw_uid,'user':os.environ['USER']}
	this_user_details.update(gname=grp.getgrgid(this_user_details['gid']).gr_name)
	#---! note that debian comes with a group "users" so we use groupmod but this might not be true on all
	texts += [('su',
		"RUN groupmod -g %(gid)d %(gname)s\n"%this_user_details+
		"RUN useradd -m -u %(uid)d -g %(gid)d %(user)s\nUSER %(user)s\nWORKDIR /home/%(user)s\n"
		%this_user_details)]
	#---never rebuild if unnecessary (docker builds are extremely quick but why waste the time)
	#---we use the texts of the docker instead of timestamps, since users might be updating other parts 
	#---...of the config file pretty frequently
	if name in docker_history.keys():
		if docker_history[name]['texts']==texts:
			raise Exception(('the docker called "%s" has already been built '+
				'and the instructions have not changed')%name)
	#---record the history for docker_history
	updates = {name:[]}
	#---loop over stages, each of which gets a separate image
	for stage,(stage_name,text) in enumerate(texts):
		start_time = time.time()
		#---prepare names
		print('[STATUS] processing %s, stage %d: %s'%(name,stage,stage_name))
		#---subsequent stages depend on the previous one so we prepend it
		if stage>0: text = 'FROM %s/%s\n'%(username,image_name)+text
		image_name = '%s-s%d'%(name,stage)
		if stage==len(texts)-1: image_name = name
		print('[STATUS] image name: %s'%image_name)
		print('\n'.join(['[CONFIG] | %s'%i for i in text.splitlines()]))
		docker_fn = os.path.join(build_dn,'Dockerfile-%s'%image_name)
		#---write the docker file
		with open(docker_fn,'w') as fp: fp.write(text)
		#---generate the image
		cmd = 'docker build -t %s/%s -f %s %s'%(
			username,image_name,docker_fn,
			os.path.join(build_dn,''))
		print('[STATUS] running "%s"'%cmd)
		subprocess.check_call(cmd,shell=True)
		elapsed = '%.1f min'%((time.time()-start_time)/60.)
		print('[TIME] elapsed: %s'%elapsed)
		updates[name].append(dict(stage=stage,name=stage_name,image=image_name,elapsed=elapsed))
	ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y.%m.%d.%H%M')
	#---save to the history with a docker style in contrast to a test style
	docker_history[name] = dict(style='docker',when=ts,series=updates,texts=texts)
	config.update(docker_history=docker_history)
	write_config(config)

def test(*sigs,**kwargs):
	"""
	Run a testset in a docker.
	"""
	build_dn = kwargs.pop('build','docker_builds')
	username = kwargs.pop('username','biophyscode')
	config_fn = kwargs.pop('config',None)
	if config_fn==None: config_fn = read_config().get('docks_config','docker_config.py')
	mods_fn = kwargs.pop('mods',None)
	if kwargs: raise Exception('unprocessed kwargs %s'%kwargs)
	#---get the interpreted docker configuration
	instruct = interpret_docker_instructions(config=config_fn,mods=mods_fn)
	#---use the sigs list to select a test set
	tests = instruct.get('tests',{})
	keys = [key for key in tests if set(sigs)==set(key.split())]
	if len(keys)!=1: 
		raise Exception('cannot find a unique key in the testset for sigs %s: %s'%(sigs,tests.keys()))
	else: name = keys[0]
	docker_execute_local(config_fn=config_fn,**tests[name])	

def docker_execute_local(**kwargs):
	"""
	Run a testset on disk.
	"""
	#---check keys here
	keys_docker_local = ('docker','where','script','config_fn')
	keys_docker_local_visit = ('docker','where','visit','config_fn')
	keys_docker_local_opts = ('once','preliminary','collect files',
		'notes','mounts','container_user','container_site')
	keysets = {
		(keys_docker_local,keys_docker_local_opts):'docker_local',
		(keys_docker_local_visit,keys_docker_local_opts):'docker_local',}
	choices = [val for key,val in keysets.items() if 
		all([i in kwargs for i in key[0]]) and 
		all([i in key[0]+key[1] for i in kwargs])]
	fail = 'docker_execute_local failed to route these instructions: %s'%kwargs
	#---default container user and site
	kwargs['container_user'] = kwargs.get('container_user','biophyscode')
	kwargs['container_site'] = kwargs.get('container_user','/root')
	if len(choices)!=1: raise Exception(fail)
	if choices[0]=='docker_local': docker_local(**kwargs)
	else: raise Exception(fail)

def docker_local(**kwargs):
	"""
	Use one of the dockers we have prepared to 
	"""
	config_fn = kwargs.pop('config_fn','docker_config.py')
	mods_fn = kwargs.pop('mods_fn',None)
	#---get the docker history
	config = read_config()
	docker_history = config.get('docker_history',{})
	testset_history = config.get('testset_history',{})
	#---check if the docker is ready
	docker_name = kwargs.get('docker')
	#---if the docker has never been made we make it
	#---! this does not handle the possibility that the texts have changed
	if docker_name not in docker_history: 
		docker(name=docker_name,config=config_fn,mods=mods_fn)
	#---check for once
	do_once = kwargs.get('once',False)
	#---check for an identical event in the testset history
	if do_once:
		#---we only continue if the kwargs are not stored in the testset history
		#---...which means that any change to the instructions triggers a remake 
		#---...but many "do once" routines will fail if you try to rerun them with slightly different settings
		#---...so it is best to be mindful of making changes to one-time-only executions
		kwargs_no_notes = copy.deepcopy(kwargs)
		kwargs_no_notes.pop('notes',None)
		events = testset_history.get('events',[])
		if any([i==kwargs_no_notes for i in events]): 
			print('[STATUS] found an exact match for this test so we are exiting')
			return
	#---check that the location is ready
	spot = os.path.abspath(os.path.expanduser(kwargs.get('where')))
	if not os.path.isdir(spot): 
		try: os.mkdir(spot)
		except Exception as e: 
			raise Exception('exception is: %s. you might need to mkdir. we failed to make %s'%(e,spot))
	else: print('[STATUS] found %s'%spot)
	#---run the custom requirements script
	if 'preliminary' in kwargs:
		import tempfile
		script = tempfile.NamedTemporaryFile(delete=True)
		with open(script.name,'w') as fp: fp.write(kwargs['preliminary'].replace('\\n', '\n'))
		subprocess.check_call('bash %s'%fp.name,shell=True)
	#---collect local files
	for key,val in kwargs.get('collect files',{}).items():
		shutil.copyfile(os.path.join(os.path.dirname(config_fn),key),os.path.join(spot,val))
	#---write the testset to the top directory. this is a transient file
	if 'script' in kwargs and kwargs.get('visit',False):
		raise Exception('found script and visit')
	elif 'script' in kwargs:
		script_header = '#!/bin/bash\nset -e\n\n'
		with open(os.path.join(spot,'script-testset.sh'),'w') as fp: fp.write(script_header+kwargs['script'])
		testset_fn = 'script-testset.sh'
	elif kwargs.get('visit',True): testset_fn = None
	else: raise Exception('need either script or visit')
	#---! by default we work in the home directory of the user. this needs documented
	user = os.environ['USER']
	container_site = os.path.join('/home/%s'%user)
	#---! container_user is hardcoded here and in the defaults for building the docker
	container_user = 'biophyscode'
	#---prepare the run settings
	run_settings = dict(user=user,host_site=spot,
		container_site=container_site,container_user=container_user,image=docker_name,
		testset_file=testset_fn,mounts_extra='')
	#---extra mounts
	for mount_from,mount_to in kwargs.get('mounts',{}).items():
		run_settings['mounts_extra'] += ' -v %s:%s'%(mount_from,os.path.join('/home/%s'%user,mount_to))
	#---run the docker
	cmd = (("docker run --rm -it "+
		"-u %(user)s -v %(host_site)s:%(container_site)s%(mounts_extra)s "+
		"%(container_user)s/%(image)s ")%run_settings+(
		"bash %(container_site)s/%(testset_file)s"%run_settings
			if run_settings['testset_file']!=None else ""))
	subprocess.check_call(cmd,shell=True)
	#---clean up the testset script
	if testset_fn!=None: 
		try: os.remove(os.path.join(spot,testset_fn))
		except: pass
	#---clean up external mounts
	#---! this is a rare delete command. add a confirm step?
	for mount_dn in [os.path.join(spot,i) for i in kwargs.get('mounts',{}).values()]:
		print('[STATUS] clearning docker mount directory %s'%mount_dn)
		shutil.rmtree(mount_dn)
	#---register this in the config if it runs only once
	if do_once:
		testset_history['events'] = testset_history.get('events',[])
		event = copy.deepcopy(kwargs)
		#---never save the notes
		event.pop('notes',None)
		testset_history['events'].append(event)
		config.update(testset_history=testset_history)
		write_config(config)
