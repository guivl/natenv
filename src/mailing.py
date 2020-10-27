import base64
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email
from python_http_client.exceptions import HTTPError


def get_user_id(session_id):
    return session_id.split('-')[0]


def get_customer_email(user_id):
    return 'guih.lana@gmail.com'


def get_authorization_send(user_id):
    return True


def email(email_customer, discount, cart_price):
    sg = SendGridAPIClient(os.environ.get['SENDGRID_API_KEY'])

    if discount:
        html_content = "<p>Seu carrinho de {1} está esperando. aqui está seu desconto para finalizar a compra.</p>".format(
            cart_price)
    else:
        html_content = "<p>Seu carrinho de {1} está esperando</p>".format(cart_price)

    message = Mail(
        to_emails=email_customer,
        from_email=Email('no-reply@example.com', "No-Reply: Natura"),
        subject="Psiu! Não esquece de nós.",
        html_content=html_content
    )
    message.add_bcc("guih.lana@gmail.com")

    try:
        response = sg.send(message)
        return response.status_code
        # expected 202 Accepted

    except HTTPError as e:
        return e


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    aux = pubsub_message.replace('(', '').replace(')', '')
    session_id = aux.split(',')[0]
    price = int(aux.split(',')[1])

    email_customer = get_customer_email(session_id)
    can_send = get_authorization_send(get_user_id(session_id))
    if price > 30 and can_send:
        email(email_customer, discount=True, cart_price=price)
    else:
        email(email_customer, discount=False, cart_price=price)
