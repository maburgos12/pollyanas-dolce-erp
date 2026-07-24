from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse


User = get_user_model()


class MermaCaptureCsrfTests(TestCase):
    def test_capture_form_repairs_csrf_field_before_native_multipart_submit(self):
        user = User.objects.create_superuser(
            username="direccion.mermas",
            email="direccion@example.com",
            password="test-only",
        )
        self.client.force_login(user)

        response = self.client.get(reverse("mermas:app"))

        self.assertContains(response, 'id="merma-capture-form"')
        self.assertContains(response, 'form.addEventListener("formdata"')
        self.assertContains(response, 'formData.set("csrfmiddlewaretoken", token)')
