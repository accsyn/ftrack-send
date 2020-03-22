# :coding: utf-8
#
# Accsyn send Action
#
# 	This Action sends components from one location to another using Accsyn:
#	
#	- Harvests all components beneath the selected entities.
#   - Consider 'ftrack.unmanaged' and custom locations.
#   - Uses paths containing project code (stripping prefix before project code).
#   - Paths are assumed beeing relative Accsyn default root share.
#
# Author: Henrik Norin, Accsyn/HDR AB, (c)2020
# 

import json
import logging
import threading
import traceback
import time

import ftrack_api

import accsyn_api

identifier = 'ftrackaccsyn_v1.action'

class AccsynSendAction():
	def __init__(self):
		self.session = ftrack_api.Session(auto_connect_event_hub=True)
		#self.session = ftrack_api.Session(auto_connect_event_hub=True)
		self.identifier = "AccsynSendAction_v1"
		self.logger = logging.getLogger(
			__name__ + '.' + self.__class__.__name__
		)
		self.excluded_locations = [
			'ftrack.origin', 
			'ftrack.connect', 
			'ftrack.unmanaged', 
			'ftrack.server', 
			'ftrack.review', 
		]

	def register(self):
		self.session.event_hub.subscribe(
				'topic=ftrack.action.discover',
				self.discover
		)

		self.session.event_hub.subscribe (
				'topic=ftrack.action.launch and data.actionIdentifier={0}'.format(
						self.identifier
				),
				self.launch
		)

	def discover(self, event):
		data = event['data']

		# Can only be run on tasks or versions
		selection = data.get('selection', [])
		self.logger.info('(AS) Discover; Got selection: {0}'.format(selection))

		if len(selection) == 0:
			return self.log_and_return("(AS) Cannot run Action - nothing selected!",True)

		return {
			'items': [{
					'label': 'Accsyn Send',
					'actionIdentifier': self.identifier
			}]
		}

	def log_and_return(self, s, retval=False):
		logging.info(s)
		return {
				'success': retval,
				'message': s
		}

	def launch(self, event):
		#self.session._local_cache.clear()
		logging.info("(AS) Launch; Event items: %s"%str(event.items()))

		selection = event['data'].get('selection', [])
		self.logger.info('(AS) Launch; Got selection: {0}'.format(selection))

		if 'values' in event['data']:
			values = event['data']['values']

			# Check user input
			source_location_name = values['source_location']
			destination_location_name = values['destination_location']
			
			if (source_location_name == destination_location_name):
				return self.log_and_return('Source and destination location are the same!'%(),False)

			thread = threading.Thread(target=self.threaded_run, args=(event, selection))
			thread.start()

			#self.threaded_run(event, selection)
			return self.log_and_return('Component transfer of %d entities(s) initiated, check Ftrack job for progress!'%(len(selection)),True)
		else:
			# Internal ftrack locations we are not interested in. 

			widgets = [
				{
					'value':"This Action sends components from one location to another using Accsyn:<br><br>"+
						"<ul>"+
							"<li>Harvests all components beneath the selected entities.</li>"+
							"<li>Consider 'ftrack.unmanaged' and custom locations.</li>"+
							"<li>Uses paths containing project code (stripping prefix before project code).</li>"+
							"<li>Paths are assumed beeing relative Accsyn default root share.</li>"
						+"</ul>",
					'type':"label"
				},
				{
					'label': 'source location',
					'data': [],
					'name': 'source_location',
					'type': 'enumerator'
				},
				{
					'label': 'destination location',
					'data': [],
					'name': 'destination_location',
					'type': 'enumerator'
				},

			]

			# Featch all locations
			for location in self.session.query('Location').all():
				logging.info("Got location: %s(%s)"%(location['name'], location['id']))

				if location['name'] in self.excluded_locations:
					# Remove source location as well as ftrack default ones.
					continue

				widgets[-2]['data'].append(
					{
						'label': location['name'],
						'value': location['name']
					}
				)

				widgets[-1]['data'].append(
					{
						'label': location['name'],
						'value': location['name']
					}
				)

			return {'items': widgets }
 
	# def async(fn):
	# 	'''Run *fn* asynchronously.'''
	# 	def wrapper(*args, **kwargs):
	# 			thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
	# 			thread.start()
	# 	return wrapper

	def threaded_run(self, event, entities):
		'''Run migration of *project_id* from *source_location* 
		to *destination_location*.
		'''
		session = ftrack_api.Session(auto_connect_event_hub=False)
		accsyn_session = accsyn_api.Session()

		logger = logging.getLogger(__name__ + '.' + self.__class__.__name__ + ".thread")
		job = None
		user = event['source']['user']

		def info(s):
			logger.info(s)
			if job:
				job['data'] = json.dumps({'description': s})
				session.commit()
			return s

		def web_message(s):
			session.event_hub.publish(
				ftrack_api.event.base.Event(
					topic='ftrack.action.trigger-user-interface',
					data=dict(
						type='message',
						success=False,
						message=(s)
					),
					target='applicationId=ftrack.client.web and user.id="{0}"'.format(user['id'])
				),
				on_error='ignore'
			)

		def error(s):
			info("[ERROR] %s"%s)
			web_message("[ERROR] %s"%s)
			return s

		values = event['data']['values']

		source_location_name = values['source_location']
		destination_location_name = values['destination_location']
			
		info("Creating Ftrack job..")

		# Create a new running Job.		
		job = session.create(
			'Job',
			{
				'user': session.get('User', user['id']),
				'status': 'running',
				'data': json.dumps({
					'description': "Initialising Accsyn send..."
					}
				)
			}
		)
		session.commit()

		job_final_status = "done"
		component_count = 0
		try:
			info("Fetching locations..")

			# Get the source location entity.
			source_location = session.query(
				'Location where name is "{}"'.format(source_location_name)
			).one()

			assert (not source_location is None),("No such source location!")

			# Get the destination location entity.
			destination_location = session.query(
				'Location where name is "{}"'.format(destination_location_name)
			).one()

			assert (not destination_location is None),("No such destination location!")

			info("Harvesting components..")

			components_and_paths = []
			all_components = []

			project_id = None

			for entity in entities:
				# Collect all the components attached to the selected entity
				if entity['entityType'] == "show":
					all_components.extend(session.query(
						'Component where version.asset.parent.project_id is "{0}"'.format(
							entity['entityId']
						)
					).all())
					project_id = entity['entityId']
				elif entity['entityType'] == "list":
					list_ = session.query(
						'List where id is "{0}"'.format(
							entity['entityId']
						)
					).one()
					# Fetch components from each item in list
					for e in list_['items' if 'items' in list_ else 'review_session_objects']:
						#o = e
						#for key in sorted(o.keys()): logging.info("   %s: %s"%(key, o[key]))
						all_components.extend(session.query(
							'Component where version.asset.parent.id is "{0}" or version.asset.parent.parent.id is "{0}" or version.asset.parent.parent.parent.id is "{0}" or version.task.id is "{0}" or version.id is "{0}"'.format(
								e['id']
							)
						).all())
				else:
					# shot/assetbuild or sequence or episode or task
					# TODO: support deeper structures
					all_components.extend(session.query(
						'Component where version.asset.parent.id is "{0}" or version.asset.parent.parent.id is "{0}" or version.asset.parent.parent.parent.id is "{0}" or version.task.id is "{0}" or version.id is "{0}"'.format(
							entity['entityId']
						)
					).all())

			if len(all_components) == 0:
				error("No components found!")
				job_final_status = "failed"
				return

			if project_id is None:
				project_id = all_components[0]['version']['asset']['parent']['project_id']
			project = session.query("Project where id={0}".format(project_id)).one()

			info("Evaluating paths (project: %s), removing unsendable components of %d.."%(project['name'], len(all_components)))

			# Filter out components like web playables etc, evaulate paths
			for component in all_components:
				p = None
				for d in component['component_locations']:
					location = d['location']
					#o = location
					#for key in sorted(o.keys()): logging.info("%s: %s"%(key, o[key]))
					if location['name'] != "ftrack.unmanaged" and location['name'] in self.excluded_locations:
						continue
					try:
						p = location.get_filesystem_path(component)
					except:
						logging.warning("   %s@%s; %s"%(component['name'], location['name'], traceback.format_exc()))
						continue
					if 0<len(p or ""):
						break
				if len(p or "") == 0:
					logging.warning("   %s; Empty path!"%(component['name']))
					continue

				idx = p.lower().find(project['name'].lower())
				if 0<=idx:
					p = p[idx:]
				else:
					logging.warning("   %s; Path '%s' could be evaluated, does not contain project code!"%(component['name'], p))
					continue

				components_and_paths.append((component, p))

			if len(components_and_paths) == 0:
				error("[ERROR] No components left after extracting paths!")
				job_final_status = "failed"
				return

			info("Building Accsyn job out of %d component(s).."%len(components_and_paths))

			accsyn_job_data = {
				'code':"Transfer of %d components from %s to %s"%(len(components_and_paths), source_location['name'], destination_location['name']),
				'tasks':[],
				'mirror_paths':True
			}
			
			for (component,p) in components_and_paths:
				logging.info("   Adding component '%s'(%s), path: %s"%(component['name'], component['id'], p))
				accsyn_job_data['tasks'].append({
					'source':"site=%s:%s"%(source_location['name'], p),
					'destination':"site=%s"%(destination_location['name'])
				})
				component_count += 1


			info("Submitting job to Accsyn (JSON: %s).."%(accsyn_job_data))

			j = accsyn_session.create("Job",accsyn_job_data)
			
			info("Submitted (id: %s).."%(j['id']))

			while True:
				time.sleep(2)
				job_data = accsyn_session.find_one("Job WHERE id={0}".format(j['id']))
				info("%s; %s, %s MB/s, %s%%, etr: %s"%(job_data['code'], job_data['status'], job_data['speed'], job_data['progress'], job_data.get('etr', "")))
				if job_data['status'] in ['done','failed','aborted']:
					if job_data['status'] in ['done']:
						web_message(info("Accsyn job finished successfully!"))
					elif job_data['status'] in ['aborted']:
						web_message(info("[WARNING] Accsyn job were aborted."))
					elif job_data['status'] in ['failed']:
						error("Accsyn job FAILED! Check Accsyn for clues!")
						job_final_status = "failed"
					break

			# Set job status as done now
		except Exception as e:
			info(traceback.format_exc())
			error("Accsyn send CRASHED! Details: %s"%str(e))
			job_final_status = "failed"
		finally:
			# This will notify the user in the web ui.
			job['status'] = job_final_status
			session.commit()

		return component_count


if __name__ == '__main__':

	# To be run as standalone code.
	logging.basicConfig(level=logging.INFO)
	
	asa = AccsynSendAction()
	asa.register()

	# Wait for events
	logging.info(
		'Registered actions and listening for events. Use Ctrl-C to abort.'
	)

	asa.session.event_hub.connect()
	asa.session.event_hub.wait()




