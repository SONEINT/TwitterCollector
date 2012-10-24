package edu.isi.webserver;

public class WebAppManager {
	public enum REQUEST_PARAMETER {
		seedHashTagsFile, seedUsersFile, seedUsersList, seedHashTagsList
	}
	
	public enum SERVLET_CONTEXT_ATTRIBUTE {
		appConfig, statsMgr
	}
}
