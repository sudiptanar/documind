package com.documind.notification.email;

/** An interface so tests use a fake and AWS can swap SMTP for the SES SDK without touching listeners. */
public interface EmailSender {

    void send(String to, String subject, String body);
}
