package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"log"
	"strconv"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
	"github.com/tarm/serial"
)

type InstantaneousDemand struct {
	XMLName             xml.Name `xml:"InstantaneousDemand"`
	DeviceMacId         string   `xml:"DeviceMacId"`
	MeterMacId          string   `xml:"MeterMacId"`
	TimeStamp           string   `xml:"TimeStamp"`
	Demand              string   `xml:"Demand" validate:"required,hexadecimal"`
	Multiplier          string   `xml:"Multiplier" validate:"required,hexadecimal"`
	Divisor             string   `xml:"Divisor" validate:"required,hexadecimal"`
	DigitsRight         string   `xml:"DigitsRight"`
	DigitsLeft          string   `xml:"DigitsLeft"`
	SuppressLeadingZero string   `xml:"SuppressLeadingZero"`
}

type CurrentSummationDelivered struct {
	XMLName             xml.Name `xml:"CurrentSummationDelivered"`
	DeviceMacId         string   `xml:"DeviceMacId"`
	MeterMacId          string   `xml:"MeterMacId"`
	TimeStamp           string   `xml:"TimeStamp"`
	SummationDelivered  string   `xml:"SummationDelivered" validate:"required,hexadecimal"`
	SummationReceived   string   `xml:"SummationReceived" validate:"required,hexadecimal"`
	Multiplier          string   `xml:"Multiplier" validate:"required,hexadecimal"`
	Divisor             string   `xml:"Divisor" validate:"required,hexadecimal"`
	DigitsRight         string   `xml:"DigitsRight"`
	DigitsLeft          string   `xml:"DigitsLeft"`
	SuppressLeadingZero string   `xml:"SuppressLeadingZero"`
}

func loadConfiguration() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.AddConfigPath("/etc/emu2mqtt/")
	viper.AddConfigPath("$HOME/.emu2mqtt")
	viper.AddConfigPath(".")

	viper.SetDefault("MQTT_HOST", "127.0.0.1")
	viper.SetDefault("MQTT_PORT", "1883")
	viper.SetDefault("SERIAL_BAUD", 115200)
	viper.SetDefault("SERIAL_PORT", "/dev/serial/by-id/usb-Rainforest_Automation__Inc._RFA-Z105-2_HW2.7.3_EMU-2-if00")

	err := viper.ReadInConfig()
	if err != nil { // Handle errors reading the config file
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			viper.AutomaticEnv()
		} else {
			log.Fatal("fatal error config file: %w", err)
		}
	}
}

func connectMQTT() mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%s", viper.GetString("MQTT_HOST"), viper.GetString("MQTT_PORT")))
	opts.SetUsername(viper.GetString("MQTT_USERNAME"))
	opts.SetPassword(viper.GetString("MQTT_PASSWORD"))
	opts.SetClientID("emu2mqtt")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	return client
}

func setupMQTTDiscovery(m mqtt.Client) {
	m.Publish("homeassistant/sensor/meter_power_demand/config", 0, true, `
	{
		"name": "Meter Power Demand",
		"unique_id": "meter_power_demand",
		"device_class": "power",
		"state_topic": "homeassistant/sensor/meter_power_demand/state",
		"state_class": "measurement",
		"unit_of_measurement": "W"
	}`)
	m.Publish("homeassistant/sensor/meter_total_energy_delivered/config", 0, true, `
	{
		"name": "Meter Total Energy Delivered",
		"unique_id": "meter_total_energy_delivered",
		"device_class": "energy",
		"state_topic": "homeassistant/sensor/meter_total_energy_delivered/state",
		"state_class": "total_increasing",
		"unit_of_measurement": "kWh"
	}`)
	m.Publish("homeassistant/sensor/meter_total_energy_received/config", 0, true, `
	{
		"name": "Meter Total Energy Received",
		"unique_id": "meter_total_energy_received",
		"device_class": "energy",
		"state_topic": "homeassistant/sensor/meter_total_energy_received/state",
		"state_class": "total_increasing",
		"unit_of_measurement": "kWh"
	}`)
}

func publishEnergy(m mqtt.Client, delivered, received string) {
	fmt.Println("Publishing Energy:", delivered, received)
	if delivered != "" {
		m.Publish("homeassistant/sensor/meter_total_energy_delivered/state", 0, false, delivered)
	}
	if received != "" {
		m.Publish("homeassistant/sensor/meter_total_energy_received/state", 0, false, received)
	}
}

func publishPower(m mqtt.Client, demand string) {
	fmt.Println("Publishing Power:", demand)
	if demand != "" {
		m.Publish("homeassistant/sensor/meter_power_demand/state", 0, false, demand)
	}
}

func connectSerial() *serial.Port {
	c := &serial.Config{Name: viper.GetString("SERIAL_PORT"), Baud: viper.GetInt("SERIAL_BAUD")}
	s, err := serial.OpenPort(c)
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func scanSerial(s *serial.Port, m mqtt.Client) {
	var instantaneousDemand InstantaneousDemand
	var currentSummationDelivered CurrentSummationDelivered
	var demand, delivered, received string

	scanner := bufio.NewScanner(s)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if i := strings.Index(string(data), "</InstantaneousDemand>\r\n"); i >= 0 {
			return i + 24, data[0 : i+24], nil
		}
		if i := strings.Index(string(data), "</CurrentSummationDelivered>\r\n"); i >= 0 {
			return i + 30, data[0 : i+30], nil
		}
		if i := strings.Index(string(data), "</TimeCluster>\r\n"); i >= 0 {
			return i + 16, data[0 : i+16], nil
		}

		return 0, nil, nil
	}

	scanner.Split(split)
	buf := make([]byte, 2)
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	v := validator.New()

	for scanner.Scan() {
		switch scanner.Text()[1] {
		case 'I':
			xml.Unmarshal([]byte(scanner.Text()), &instantaneousDemand)
			err := v.Struct(instantaneousDemand)
			if err != nil {
				log.Print("Skipping incomplete XML:", err)
				continue
			}
			i, err := strconv.ParseInt(instantaneousDemand.Demand, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			mult, err := strconv.ParseInt(instantaneousDemand.Multiplier, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			div, err := strconv.ParseInt(instantaneousDemand.Divisor, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			demand = fmt.Sprintf("%v", int(float64(int32(i))*float64(mult)/float64(div)*1000))
			publishPower(m, demand)
		case 'C':
			xml.Unmarshal([]byte(scanner.Text()), &currentSummationDelivered)
			err := v.Struct(currentSummationDelivered)
			if err != nil {
				log.Print("Skipping incomplete XML:", err)
				continue
			}
			d, err := strconv.ParseInt(currentSummationDelivered.SummationDelivered, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			r, err := strconv.ParseInt(currentSummationDelivered.SummationReceived, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			mult, err := strconv.ParseInt(currentSummationDelivered.Multiplier, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			div, err := strconv.ParseInt(currentSummationDelivered.Divisor, 0, 64)
			if err != nil {
				log.Fatal("ERROR parsing XML:", err)
			}
			delivered = fmt.Sprintf("%.3f", float64(int32(d))*float64(mult)/float64(div))
			received = fmt.Sprintf("%.3f", float64(int32(r))*float64(mult)/float64(div))
			publishEnergy(m, delivered, received)
		case 'T':
			// ignored
		default:
			log.Fatal("Unexpected case")
		}
	}
}

func main() {

	loadConfiguration()

	m := connectMQTT()
	setupMQTTDiscovery(m)

	s := connectSerial()
	scanSerial(s, m)

}
