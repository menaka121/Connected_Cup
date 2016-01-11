package org.wso2.carbon.device.mgt.connectedcup.controller.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.annotations.api.API;
import org.wso2.carbon.apimgt.annotations.device.DeviceType;
import org.wso2.carbon.apimgt.annotations.device.feature.Feature;
import org.wso2.carbon.device.mgt.common.DeviceIdentifier;
import org.wso2.carbon.device.mgt.common.DeviceManagementException;
import org.wso2.carbon.device.mgt.connectedcup.controller.service.impl.dto.DeviceJSON;
import org.wso2.carbon.device.mgt.connectedcup.controller.service.impl.transport.ConnectedCupMQTTConnector;
import org.wso2.carbon.device.mgt.connectedcup.controller.service.impl.util.ConnectedCupServiceUtils;
import org.wso2.carbon.device.mgt.connectedcup.plugin.constants.ConnectedCupConstants;
import org.wso2.carbon.device.mgt.iot.DeviceValidator;
import org.wso2.carbon.device.mgt.iot.exception.DeviceControllerException;
import org.wso2.carbon.device.mgt.iot.sensormgt.SensorDataManager;
import org.wso2.carbon.device.mgt.iot.sensormgt.SensorRecord;
import org.wso2.carbon.device.mgt.iot.transport.TransportHandlerException;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Calendar;


/**
 * Controller service for Connected Cup.
 * Read sensor data from device
 * Three methods
 *      - ReadTemperature
 *      - ReadCoffeeLevel
 *      - OrderCoffee
 *
 * There are 2 stream definitions.
 *      1. Temperature
 *      2. Coffee level
 *
 */

@API(name="connectedcup", version = "1.0", context = "/connectedcup")
@DeviceType("connectedcup")
public class ConnectedCupControllerService {

    private static Log log = LogFactory.getLog(ConnectedCupControllerService.class);
    private static final String SUPER_TENANT = "carbon.super";
    private ConnectedCupMQTTConnector connectedCupMQTTConnector;

    /**
     * @param owner
     * @param deviceId
     * @param response
     * @return
     */
    @Path("controller/readlevel")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Feature(code = "readlevel", name = "Humidity", type = "monitor",
            description = "Read Humidity Readings from Connected Cup")
    public SensorRecord requestHumidity(@HeaderParam("owner") String owner,
                                        @HeaderParam("deviceId") String deviceId,
                                        @Context HttpServletResponse response) {
        SensorRecord sensorRecord = null;
        DeviceValidator deviceValidator = new DeviceValidator();
        try {
            if (!deviceValidator.isExist(owner, SUPER_TENANT, new DeviceIdentifier(
                    deviceId, ConnectedCupConstants.DEVICE_TYPE))) {
                response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            }
        } catch (DeviceManagementException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }


        if (log.isDebugEnabled()) {
            log.debug("Sending request to read humidity value of device [" + deviceId + "] via MQTT");
        }

        try {


            String mqttResource = ConnectedCupConstants.LEVEL_CONTEXT.replace("/", "");
            connectedCupMQTTConnector.publishDeviceData(owner, deviceId, mqttResource, "");

            sensorRecord = SensorDataManager.getInstance().getSensorRecord(deviceId,
                                                                           ConnectedCupConstants.SENSOR_LEVEL);
        } catch ( DeviceControllerException | TransportHandlerException e ) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response.setStatus(Response.Status.OK.getStatusCode());
        return sensorRecord;
    }

    /**
     * @param owner
     * @param deviceId
     * @param response
     * @return
     */
    @Path("controller/readtemperature")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Feature( code="readtemperature", name="Temperature", type="monitor",
            description="Request Temperature reading from Connected Cup")
    public SensorRecord requestTemperature(@HeaderParam("owner") String owner,
                                           @HeaderParam("deviceId") String deviceId,
                                           @Context HttpServletResponse response) {
        SensorRecord sensorRecord = null;

        DeviceValidator deviceValidator = new DeviceValidator();
        try {
            if (!deviceValidator.isExist(owner, SUPER_TENANT,
                                         new DeviceIdentifier(deviceId, ConnectedCupConstants.DEVICE_TYPE))) {
                response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            }
        } catch (DeviceManagementException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }


        if (log.isDebugEnabled()) {
            log.debug("Sending request to read connected cup temperature of device " +
                      "[" + deviceId + "] via MQTT");
        }

        try {


            String mqttResource = ConnectedCupConstants.TEMPERATURE_CONTEXT.replace("/", "");
            connectedCupMQTTConnector.publishDeviceData(owner, deviceId, mqttResource, "");

            sensorRecord = SensorDataManager.getInstance().getSensorRecord(deviceId,
                                                                           ConnectedCupConstants.SENSOR_TEMPERATURE);
        } catch ( DeviceControllerException | TransportHandlerException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response.setStatus(Response.Status.OK.getStatusCode());
        return sensorRecord;
    }

    /**
     * @param dataMsg
     * @param response
     */
    @Path("controller/push_temperature")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void pushTemperatureData(final DeviceJSON dataMsg,
                                    @Context HttpServletResponse response) {
        String deviceId = dataMsg.deviceId;
        String deviceIp = dataMsg.reply;
        float temperature = dataMsg.value;

        String registeredIp = deviceToIpMap.get(deviceId);

        if (registeredIp == null) {
            log.warn("Unregistered IP: Temperature Data Received from an un-registered IP " +
                     deviceIp + " for device ID - " + deviceId);
            response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
            return;
        } else if (!registeredIp.equals(deviceIp)) {
            log.warn("Conflicting IP: Received IP is " + deviceIp + ". Device with ID " + deviceId +
                     " is already registered under some other IP. Re-registration required");
            response.setStatus(Response.Status.CONFLICT.getStatusCode());
            return;
        }
        SensorDataManager.getInstance().setSensorRecord(deviceId, ConnectedCupConstants.SENSOR_TEMPERATURE,
                                                        String.valueOf(temperature),
                                                        Calendar.getInstance().getTimeInMillis());

        if (!ConnectedCupServiceUtils.publishToDAS(dataMsg.owner, dataMsg.deviceId, dataMsg.value)) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

    }

    /**
     * @param dataMsg
     * @param response
     */
    @Path("controller/push_level")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void pushTemperatureData(final DeviceJSON dataMsg,
                                    @Context HttpServletResponse response) {
        String deviceId = dataMsg.deviceId;
        String deviceIp = dataMsg.reply;
        float temperature = dataMsg.value;

        String registeredIp = deviceToIpMap.get(deviceId);

        if (registeredIp == null) {
            log.warn("Unregistered IP: Temperature Data Received from an un-registered IP " +
                     deviceIp + " for device ID - " + deviceId);
            response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
            return;
        } else if (!registeredIp.equals(deviceIp)) {
            log.warn("Conflicting IP: Received IP is " + deviceIp + ". Device with ID " + deviceId +
                     " is already registered under some other IP. Re-registration required");
            response.setStatus(Response.Status.CONFLICT.getStatusCode());
            return;
        }
        SensorDataManager.getInstance().setSensorRecord(deviceId, ConnectedCupConstants.SENSOR_LEVEL,
                                                        String.valueOf(temperature),
                                                        Calendar.getInstance().getTimeInMillis());

        if (!ConnectedCupServiceUtils.publishToDAS(dataMsg.owner, dataMsg.deviceId, dataMsg.value)) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

    }



}
