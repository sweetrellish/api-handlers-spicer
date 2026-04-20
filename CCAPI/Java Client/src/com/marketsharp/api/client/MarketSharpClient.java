package com.marketsharp.api.client;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import msharpmodel.*;
import org.restlet.data.ChallengeResponse;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.Form;
import org.restlet.data.Parameter;
import org.restlet.engine.Engine;
import org.restlet.engine.util.Base64;
import org.restlet.ext.odata.Query;
import org.restlet.util.Series;

public class MarketSharpClient {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Engine.getInstance().getRegisteredAuthenticators().add(new CustomAuthenticationHelper());
        MarketSharpCrmApiService service = new MarketSharpCrmApiService();
        service.setCredentials(new ChallengeResponse(ChallengeScheme.CUSTOM));
        try {
            //testLeadPaintAttachment(service);
            //testAppointmentQuery(service);
            //testAppointmentQuery2(service);
            //testJob(service);
            //testContact(service);
            //testContact2(service);
            //testContactType(service);
            //testProductType(service);
            //testProductDetail(service);
            //testInquirySourcePrimary(service);
            //testInquirySourceSecondary(service);
            //testNoteQuery(service);
            
            testService(service);
        } catch (Exception ex) {
            Logger.getLogger(MarketSharpClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private static void testLeadPaintAttachment(MarketSharpCrmApiService service)
    {
        Query<LeadPaintAttachment> leadPaintAttachmentQuery = service.createLeadPaintAttachmentQuery("/LeadPaintAttachments").top(10);
        
        for (LeadPaintAttachment leadPaintAttachment : leadPaintAttachmentQuery)
        {
            System.out.println("Attachment");
            System.out.println("ID:" + leadPaintAttachment.getId());
            System.out.println("ATTACHEMENT PATH:" + leadPaintAttachment.getAttachmentPath());
            System.out.println("ATTACHEMENT FILENAME:" + leadPaintAttachment.getAttachmentFileName());
        }
    }
    
    private static byte[] getLeadPaintFile(MarketSharpCrmApiService service, String guid) throws Exception
    {
        Series<Parameter> fargs = new Form();
        fargs.add("id", guid);
        return Base64.decode(service.invokeSimple("GetLeadPaintFile", fargs));
    }
    
    private static void testLeadPaintFileAttachment(MarketSharpCrmApiService service, String filename, String guidLeadPaintAttId)
    {
        BufferedOutputStream fout = null;
        try {
            fout = new BufferedOutputStream(new FileOutputStream(filename));
            fout.write(getLeadPaintFile(service, guidLeadPaintAttId));
            fout.close();
        } catch (Exception ex) {
            Logger.getLogger(MarketSharpClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally
        {
            try
            {
                fout.close();
            }
            catch (IOException ex)
            {
                Logger.getLogger(MarketSharpClient.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private static void testContact2(MarketSharpCrmApiService service)
    {
        Query<Contact> contactQuery = service.createContactQuery("/Contacts")
                .expand("Address")
                .filter("ContactPhone/homePhone eq 'Smith'");
        
        for (Contact contact : contactQuery)
        {
            System.out.println("Contact");
            System.out.println("ID:" + contact.getId());
            System.out.println("FIRST NAME:" + contact.getFirstName());
            System.out.println("LAST NAME:" + contact.getLastName());
        }
    }
    
    private static void testJob(MarketSharpCrmApiService service)
    {
        Query<Job> jobQuery = service.createJobQuery("/Jobs")
                .expand("Contact")
                .filter("completedDate gt datetime'2011-01-01T00:00:00'");
        
        Contact contact;
        
        for (Job job : jobQuery)
        {
            contact = job.getContact();
            
            System.out.println("Contact");
            System.out.println("ID:" + contact.getId());
            System.out.println("FIRST NAME:" + contact.getFirstName());
            System.out.println("LAST NAME:" + contact.getLastName());
            
            System.out.println("Job");
            System.out.println("ID:" + job.getId());
            System.out.println("NAME:" + job.getName());
        }
    }
    
    private static void testContactType(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT
        Query<ContactType> contactQuery = service.createContactTypeQuery("/ContactTypes");
        contactQuery = contactQuery.expand("Contact");
        contactQuery = contactQuery.filter("contactType eq '3' and Contact/isActive");
        
        //LOOP THROUGH ITERATOR
        for (ContactType contactType : contactQuery)
        {
            System.out.println("Contact");
            System.out.println("ID:" + contactType.getId());
            System.out.println("FIRST NAME:" + contactType.getContact().getFirstName());
            System.out.println("LAST NAME:" + contactType.getContact().getFirstName());
        }
    }
    
    private static void testContact(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<Contact> contactQuery = service.createContactQuery("/Contacts");

        //ENTITY VARIABLES
        List<CustomField> customFields;

        //LOOP THROUGH ITERATOR
        for (Contact contact : contactQuery)
        {
            System.out.println("Contact");
            System.out.println("ID:" + contact.getId());
            System.out.println("COMPANY ID:" + contact.getCompanyId());
            System.out.println("FIRST NAME:" + contact.getFirstName());
            System.out.println("LAST NAME:" + contact.getLastName());
            
            customFields = contact.getCustomField();
            for (CustomField customField : customFields)
            {
                System.out.println("Custom Fields");
                System.out.println("NAME:" + customField.getName());
                System.out.println("VALUE:" + customField.getValue());
            }
        }
    }
    
    private static void testProductType(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<ProductType> productTypeQuery = service.createProductTypeQuery("/ProductTypes").expand("Company");

        //ENTITY VARIABLES
        Company company;

        //LOOP THROUGH ITERATOR
        for (ProductType productType : productTypeQuery)
        {
            company = productType.getCompany();
            System.out.println("Product Type");
            System.out.println("ID:" + productType.getId());
            System.out.println("NAME:" + productType.getName());
            
            System.out.println("Company");
            System.out.println("ID:" + company.getId());
            System.out.println("NAME:" + company.getName());
        }
    }
    
    private static void testProductDetail(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<ProductDetail> productDetailQuery = service.createProductDetailQuery("/ProductDetails").expand("Company");

        //ENTITY VARIABLES
        Company company;

        //LOOP THROUGH ITERATOR
        for (ProductDetail productDetail : productDetailQuery)
        {
            company = productDetail.getCompany();
            System.out.println("Product Detail");
            System.out.println("ID:" + productDetail.getId());
            System.out.println("NAME:" + productDetail.getName());
            
            System.out.println("Company");
            System.out.println("ID:" + company.getId());
            System.out.println("NAME:" + company.getName());
        }
    }
    
    private static void testInquirySourcePrimary(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<InquirySourcePrimary> inquirySourcePrimaryQuery = service.createInquirySourcePrimaryQuery("/InquirySourcePrimaries").expand("Company");

        //ENTITY VARIABLES
        Company company;

        //LOOP THROUGH ITERATOR
        for (InquirySourcePrimary inquirySource : inquirySourcePrimaryQuery)
        {
            company = inquirySource.getCompany();
            System.out.println("Inquiry Source Primary");
            System.out.println("ID:" + inquirySource.getId());
            System.out.println("NAME:" + inquirySource.getName());
            
            System.out.println("Company");
            System.out.println("ID:" + company.getId());
            System.out.println("NAME:" + company.getName());
        }
    }
    
    private static void testInquirySourceSecondary(MarketSharpCrmApiService service)
    {
        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<InquirySourceSecondary> inquirySourceSecondaryQuery = service.createInquirySourceSecondaryQuery("/InquirySourceSecondaries").expand("Company");

        //ENTITY VARIABLES
        Company company;

        //LOOP THROUGH ITERATOR
        for (InquirySourceSecondary inquirySource : inquirySourceSecondaryQuery)
        {
            company = inquirySource.getCompany();
            System.out.println("Inquiry Source Secondary");
            System.out.println("ID:" + inquirySource.getId());
            System.out.println("NAME:" + inquirySource.getName());
            
            System.out.println("Company");
            System.out.println("ID:" + company.getId());
            System.out.println("NAME:" + company.getName());
        }
    }
    
    private static void testAppointmentQuery2(MarketSharpCrmApiService service)
    {
        Query<Appointment> appointmentsQuery = service.createAppointmentQuery("/Appointments");
        appointmentsQuery = appointmentsQuery.expand("Inquiry");
        appointmentsQuery = appointmentsQuery.filter("lastUpdate gt datetime'2011-12-01T15:45:01'");
        System.out.println("Pulling appointments from 2011-12-01T04:45:01.");
        appointmentsQuery = appointmentsQuery.orderBy("lastUpdate");
        
        //LOOP THROUGH ITERATOR
        Inquiry inquiry;
        int count = 0;
        for (Appointment appointment : appointmentsQuery)
        {
            count++;
            //LOAD NAVIGATION OBJECTS
            inquiry = appointment.getInquiry();

            //PRINT OUT RESULTS
            System.out.println();
            System.out.println("Appointment");
            System.out.println("ID:" + appointment.getId());
            System.out.println("TYPE:" + appointment.getType());
            System.out.println("SUBJ:" + appointment.getSubject());
            System.out.println("APPTDT:" + appointment.getAppointmentDate());
            System.out.println("LASTUPDATE:" + appointment.getLastUpdate().toString());

            if (inquiry != null) {
                System.out.println();
                System.out.println("-->Inquiry");
                System.out.println("-->ID:" + inquiry.getId());
                System.out.println("-->DESC:" + inquiry.getDescription());
                System.out.println("-->INQDT:" + inquiry.getInquiryDate());
                System.out.println("-->NOTE:" + inquiry.getNote());
            }
        }
        
        System.out.println("Record Count: " + count);
    }

    private static void testAppointmentQuery(MarketSharpCrmApiService service) {

        //CREATE QUERY OBJECT TO RETRIEVE APPOINTMENTS
        Query<Appointment> soldAppointmentsQuery = service.createAppointmentQuery("/Appointments")
                .expand("Inquiry,Inquiry/InquirySourcePrimary,Inquiry/InquirySourceSecondary,AppointmentResult,Salesperson1")
                .filter("lastUpdate gt datetime'2011-12-19T07:00:00'and AppointmentResult/sold");

        //ENTITY VARIABLES
        Inquiry inquiry;
        AppointmentResult appointmentResult;
        Employee salesperson1;
        
        //LOOP THROUGH ITERATOR
        int count = 0;
        for (Appointment appointment : soldAppointmentsQuery)
        {
            count++;
            //LOAD NAVIGATION OBJECTS
            inquiry = appointment.getInquiry();
            appointmentResult = appointment.getAppointmentResult();
            salesperson1 = appointment.getSalesperson1();

            //PRINT OUT RESULTS
            System.out.println();
            System.out.println("Appointment");
            System.out.println("ID:" + appointment.getId());
            System.out.println("TYPE:" + appointment.getType());
            System.out.println("RSLTRSN:" + appointment.getResultReason());
            System.out.println("SETDT:" + appointment.getSetDate());
            System.out.println("SUBJ:" + appointment.getSubject());
            System.out.println("APPTDT:" + appointment.getAppointmentDate());
            System.out.println("INQID:" + appointment.getInquiryId());
            System.out.println("LASTUPDATE:" + appointment.getLastUpdate().toString());

            if (inquiry != null) {
                System.out.println();
                System.out.println("-->Inquiry");
                System.out.println("-->ID:" + inquiry.getId());
                System.out.println("-->CONTACTID:" + inquiry.getContactId());
                System.out.println("-->DESC:" + inquiry.getDescription());
                if (inquiry.getInquirySourcePrimary() != null) {
                    System.out.println("-->INQSRCPRI:" + inquiry.getInquirySourcePrimary().getName());
                }
                if (inquiry.getInquirySourceSecondary() != null){
                    System.out.println("-->INQSRCSEC:" + inquiry.getInquirySourceSecondary().getName());
                }
                System.out.println("-->INQDT:" + inquiry.getInquiryDate());
                System.out.println("-->NOTE:" + inquiry.getNote());
            }

            if (appointmentResult != null) {
                System.out.println();
                System.out.println("-->AppointmentResult");
                System.out.println("-->ID:" + appointmentResult.getId());
                System.out.println("-->NAME:" + appointmentResult.getName());
                System.out.println("-->PRES:" + appointmentResult.getPresentation());
                System.out.println("-->SOLD:" + appointmentResult.getSold());
            }

            if (salesperson1 != null) {
                System.out.println();
                System.out.println("-->Salesperson1");
                System.out.println("-->ID:" + salesperson1.getId());
                System.out.println("-->COMPANYID:" + salesperson1.getCompanyId());
                System.out.println("-->NAME:" + salesperson1.getName());
            }
        }
        
        System.out.println("Record Count: " + count);
    }
    
    private static void testNoteQuery(MarketSharpCrmApiService service)
    {
         Query<Note> noteQuery = service.createNoteQuery("/Notes").top(10);
         
         for(Note note : noteQuery)
         {
             System.out.println("-->Note:" + note.getNote());
         }
    }

    private static void testService(MarketSharpCrmApiService service) {
        try {
            service.createActivityQuery("/Activities").top(10).execute();
            service.createActivityReferenceQuery("/ActivityReferences").top(10).execute();
            service.createActivityResultQuery("/ActivityResults").top(10).execute();
            service.createAdditionalContactQuery("/AdditionalContacts").top(10).execute();
            service.createAddressQuery("/Addresses").top(10).execute();
            service.createAppointmentQuery("/Appointments").top(10).execute();
            service.createC800responseLeadQuery("/C800ResponseLead").top(10).execute();
            service.createCompanyQuery("/Companies").top(10).execute();
            service.createContactPhoneQuery("/ContactPhones").top(10).execute();
            service.createContactQuery("/Contacts").top(10).execute();
            service.createContactTypeQuery("/ContactTypes").top(10).execute();
            service.createContractQuery("/Contracts").top(10).execute();
            service.createCustomFieldQuery("/CustomFields").top(10).execute();
            service.createEmployeeQuery("/Employees").top(10).execute();
            service.createEmployeeInfoQuery("/EmployeeInfoes").top(10).execute();
            service.createFutureInterestQuery("/FutureInterests").top(10).execute();
            service.createInquiryQuery("/Inquiries").top(10).execute();
            service.createInquiryStatusQuery("/InquiryStatuses").top(10).execute();
            service.createInquirySourcePrimaryQuery("/InquirySourcePrimaries").expand("Company").top(10).execute();
            service.createInquirySourceSecondaryQuery("/InquirySourceSecondaries").expand("Company").top(10).execute();
            service.createJobProductCommissionPaymentsQuery("/JobProductCommissionPayments").top(10).execute();
            service.createJobProductCommissionQuery("/JobProductCommissions").top(10).execute();
            service.createJobProductCostQuery("JobProductCosts").top(10).execute();
            service.createJobProductCostTypeQuery("JobProductCostTypes").top(10).execute();
            service.createJobProductDetailQuery("/JobProductDetails").top(10).execute();
            service.createJobProductQuery("/JobProducts").top(10).execute();
            service.createJobQuery("/Jobs").top(10).execute();
            service.createLeadPaintAttachmentQuery("/LeadPaintAttachments").top(10).execute();
            service.createLeadPaintFirmQuery("/LeadPaintFirms").top(10).execute();
            service.createLeadPaintQuery("/LeadPaints").top(10).execute();
            service.createLeadPaintRenovatorQuery("/LeadPaintRenovators").top(10).execute();
            service.createLeadPaintToFirmQuery("/LeadPaintToFirms").top(10).execute();
            service.createLeadPaintToRenovatorQuery("/LeadPaintToRenovators").top(10).execute();
            service.createLeadPaintToWorkerQuery("/LeadPaintToWorkers").top(10).execute();
            service.createLeadPaintWorkerQuery("/LeadPaintWorkers").top(10).execute();
            service.createLoanQuery("/Loans").top(10).execute();
            service.createNoteQuery("/Notes").top(10).execute();
            service.createPaymentHistoryQuery("/PaymentHistories").top(10).execute();
            service.createProcessStepQuery("/ProcessSteps").top(10).execute();
            service.createProductInterestQuery("/ProductInterests").top(10).execute();
            service.createProductDetailQuery("/ProductDetails").expand("Company").top(10).execute();
            service.createProductTypeQuery("/ProductTypes").expand("Company").top(10).execute();
            service.createProposalQuery("/Proposals").top(10).execute();
            service.createServiceOrderQuery("/ServiceOrders").top(10).execute();
            service.createSurveyQuery("/Surveys").top(10).execute();
            service.createWorkCrewQuery("/WorkCrews").top(10).execute();
        } catch (Exception ex) {
            Logger.getLogger(MarketSharpClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
