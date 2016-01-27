package com.valuepotion.analytics.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.configuration.ConfigurationFactoryBean;
import org.springframework.data.hadoop.mapreduce.ToolRunner;

public class MapReduceToolRunner {

	public void run(String jar, String mainClass, Properties properties, String[] args, String[] files, String[] libJars, String[] archives) throws Exception {
		ToolRunner toolRunner = new ToolRunner();

		ConfigurationFactoryBean configurationFactoryBean = new ConfigurationFactoryBean();
		Configuration configuration = configurationFactoryBean.getObject();
		toolRunner.setConfiguration(configuration);
		
		if (properties != null) {
			toolRunner.setProperties(properties);
		}
		
		toolRunner.setJar(getResource(jar));
		toolRunner.setToolClass(mainClass);
		
		if (files != null) {
			toolRunner.setFiles(getResources(files));
		}
		
		if (libJars != null) {
			toolRunner.setLibs(getResources(libJars));
		}

		if (archives != null) {
			toolRunner.setArchives(getResources(archives));
		}
		
		toolRunner.setArguments(args);
		
		toolRunner.setCloseFs(true);
		toolRunner.call();
	}
	
	private Resource getResource(String path) {
		File resourceFile = new File(path);
		if (resourceFile.exists()) {
			return new FileSystemResource(resourceFile);
		} else {
			return new ClassPathResource(path);
		}
	}

	private Resource[] getResources(String[] paths) {
		List<Resource> resourceList = new ArrayList<Resource>();
		
		for (int i = 0; i < paths.length; i++) {
			resourceList.add(getResource(paths[i]));
		}
		
		Resource[] resources = new Resource[resourceList.size()];
		return resourceList.toArray(resources);
	}
	
	public static void main(String[] args) throws Exception {

	}
}
