from flask.ext.wtf import Form, TextField, BooleanField
from flask.ext.wtf import Required
from flask import current_app, redirect,url_for, session, request, flash, render_template
from functools import wraps
import ldap,ldif,sys


class j_ldap:

        ldapGroupBase = "ou=Groups,o=bhntampa"

	def __init__(self,app):
		ld = self.ldap_connect(app)
		ldap_host = app.config['LDAP_HOST']
		basedn = app.config['LDAP_SEARCH_BASE']

	def ldap_connect(self,app):
		ldap.set_option(ldap.OPT_X_TLS_CACERTFILE,'/etc/bhncert')
		ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_NEVER)
		self.ld = ldap.initialize('ldaps://localhost:6333')

	def ldap_search_dn(self,username):
		filter = "uid="+username
		basedn = 'ou=People,o=bhntampa'
		r = self.ld.search_s(basedn,ldap.SCOPE_SUBTREE,filter)
		print r
		return r 
# example cn=ken,ou=SupportServices,ou=SystemsOperations,ou=TIS,ou=People,o=bhntampa

	def simple_bind(self,dn,pwd):
		try:
			self.ld.simple_bind_s(dn,pwd)
			#self.ld.unbind_s()
			print session
			return 1
		except Exception,e:
			return e

	def ldap_login(self,username,password):
		dn = self.ldap_search_dn(username)
		print dn[0][0]
		bind_rs = self.simple_bind(dn[0][0],password) 
		print bind_rs 
	        if bind_rs == 1:
			flash("login Succeded")
			session['username'] = username
			session['groups'] = dn[0][1]['groupMembership']
			return 1	
		else:
			flash("login Failed")
			return bind_rs

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
	if session['username'] != None:
            return f(*args, **kwargs)
        return redirect(url_for('login'))

    return decorated
