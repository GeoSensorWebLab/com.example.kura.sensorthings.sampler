package com.example.kura.sensorthings.sampler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sampler {

	private static final Logger s_logger = LoggerFactory.getLogger(Sampler.class);
	private static final String APP_ID = "com.example.kura.sensorthings.sampler";

	private ScheduledExecutorService m_worker;
	private ScheduledFuture<?> m_handle;

	public Sampler() {
		super();
		m_worker = Executors.newSingleThreadScheduledExecutor();
	}

	protected void activate(ComponentContext componentContext) {
		s_logger.info("Bundle " + APP_ID + " has started!");

		// cancel a current worker handle if one if active
		if (m_handle != null) {
			m_handle.cancel(true);
		}

		// schedule a new worker
		m_handle = m_worker.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Thread.currentThread().setName(getClass().getSimpleName());
				doPublish();
			}
		}, 0, 300, TimeUnit.SECONDS);
	}

	protected void deactivate(ComponentContext componentContext) {
		s_logger.info("Bundle " + APP_ID + " has stopped!");
	}

	/**
	 * Called at the configured rate to publish the next temperature
	 * measurement.
	 */
	private void doPublish() {
		s_logger.info(String.format("Sampler: Preparing a Publish Event"));

		String observationsURI = "http://example.com/OGCSensorThings/v1.0/Datastreams(240959)/Observations";
		ZonedDateTime now = ZonedDateTime.now();

		// Query the sensor value. As it is simulated, we pass in the current
		// date.
		float lux = getLux(now);

		// Get phenomenonTime as string for the Observations
		String phenomenonTime = now.format(DateTimeFormatter.ISO_INSTANT);

		// Create Observation
		createObservation(phenomenonTime, lux, observationsURI);
	}

	// Wrap the post call with error handling.
	private void createObservation(String phenomenonTime, float result, String resourceURI) {
		try {
			postObservation(phenomenonTime, result, resourceURI);
		} catch (MalformedURLException e) {
			s_logger.warn(String.format("Sampler: Cannot create Observation, as URL is malformed: %s", resourceURI));
		} catch (IOException e) {
			s_logger.warn(String.format("Sampler: Cannot create Observation, as a connection could not be opened."));
		}
	}

	// Returns a simulated Lux value for a given time of day.
	private float getLux(ZonedDateTime instant) {
		float hour = (float) instant.getHour();
		float minute = (float) instant.getMinute();
		float lux = (float) Math.sin((1.5 * Math.PI) + ((Math.PI * hour + (minute / 60)) / 12));
		return lux;
	}

	// Create a new SensorThings Observation with phenomenonTime and value, in
	// the Observations Collection for the resource at resourceURI. The
	// resource is most likely a URL to a Datastream.
	private void postObservation(String phenomenonTime, float result, String resourceURI)
			throws MalformedURLException, IOException {
		String postBody = String.format("{\"phenomenonTime\": \"%s\", \"result\": %f}", phenomenonTime, result);

		URL resourceURL = new URL(resourceURI);
		HttpURLConnection connection = (HttpURLConnection) resourceURL.openConnection();

		connection.setDoOutput(true);
		connection.setInstanceFollowRedirects(false);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");

		DataOutputStream writer = new DataOutputStream(connection.getOutputStream());
		writer.writeBytes(postBody);
		writer.flush();
		writer.close();

		int responseCode = connection.getResponseCode();
		s_logger.info(String.format("Sampler Response Code: %d", responseCode));
	}

}
