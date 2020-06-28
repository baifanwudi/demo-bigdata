package com.demo.util;


import org.apache.commons.lang3.StringUtils;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public final class DateUtils {

	private final static String TIME_PATTERN_YYYY_MM_DD = "yyyy-MM-dd";

	private final static String TIME_PATTERN_YYYY_MM = "yyyy-MM";

	public static final String TIME_PATTERN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

	public static final DateTimeFormatter TIME_FORMAT_YYYY_MM=DateTimeFormatter.ofPattern(TIME_PATTERN_YYYY_MM);

	public static final DateTimeFormatter TIME_FORMAT_YYYYMMDDHHMMSS = DateTimeFormatter.ofPattern(TIME_PATTERN_YYYYMMDDHHMMSS);

	public static final DateTimeFormatter TIME_FORMAT_YYYY_MM_DD=DateTimeFormatter.ofPattern(TIME_PATTERN_YYYY_MM_DD);

	public static final DateTimeFormatter TIME_FORMAT_YYYYMMDD=DateTimeFormatter.ofPattern("yyyyMMdd");

	public static final DateTimeFormatter TIME_FORMAT_YYYY_MM_DD_HHMMSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public static final DateTimeFormatter TIME_FORMAT_YYYY_MM_DDHHMM=DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm");

	public static String produceDateOrYesterday(String[] args) {
		return args.length < 1 ? nowAfterOrBeforeDay(-1) : args[0];
	}

	public static Date parseStringToDate(String day) {
		try {
			return org.apache.commons.lang3.time.DateUtils.parseDate(day, TIME_PATTERN_YYYY_MM_DD);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String nowDate() {
		return nowAfterOrBeforeDay(0);
	}

	public static String nowAfterOrBeforeDay(int day) {
		LocalDate localDate = LocalDate.now();
		localDate=localDate.plusDays(day);
		return localDate.format(TIME_FORMAT_YYYY_MM_DD);
	}

	public static String nowAfterOrBeforeMonth(int month) {
		LocalDate localDate = LocalDate.now();
		localDate=localDate.plusMonths(month);
		return localDate.format(TIME_FORMAT_YYYY_MM);
	}


	public static String afterOrBeforeDays(String day, int addNum) {
		LocalDate localDate=LocalDate.parse(day,TIME_FORMAT_YYYY_MM_DD);
		localDate=localDate.plusDays(addNum);
		return localDate.format(TIME_FORMAT_YYYY_MM_DD);
	}

	public static Date dateAddDay(Date date,int dayNum){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, dayNum);
		return calendar.getTime();
	}

	public static Integer getWeek(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		Integer weekDay=cal.get(Calendar.DAY_OF_WEEK)-1;
		return weekDay==0?7:weekDay;
	}

	public static String dateToString(Date date) {
		LocalDate localDateTime=date2LocalDate(date);
		return localDateTime.format(TIME_FORMAT_YYYY_MM_DD);
	}

	public static LocalDate date2LocalDate(Date date) {
		if(null == date) {
			return null;
		}
		return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
	}

	public static Date localDate2Date(LocalDate localDate) {
		if(null == localDate) {
			return null;
		}
		ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());
		return Date.from(zonedDateTime.toInstant());
	}

	public static int daysBetweenDates(Date lateDate,Date earlyDate){
		return (int)((lateDate.getTime()-earlyDate.getTime())/(1000*3600*24));
	}

	public static int calculateStrTimeToMin(String strTime) {
		if (StringUtils.isNotBlank(strTime)) {
			int hour = 0, min = 0, minIndex = 0;
			if (strTime.contains("小时")) {
				hour = Integer.parseInt(strTime.substring(0, strTime.indexOf("小时")));
				minIndex = strTime.indexOf("小时") + 2;
			} else if (strTime.contains("时")) {
				hour = Integer.parseInt(strTime.substring(0, strTime.indexOf("时")));
				minIndex = strTime.indexOf("时") + 1;
			}
			if (strTime.contains("分")) {
				min = Integer.parseInt(strTime.substring(minIndex, strTime.indexOf("分")));
			}
			if (hour > 0 || min > 0) {
				return hour * 60 + min;
			}
		}
		return 0;
	}

}
