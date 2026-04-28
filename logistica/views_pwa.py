from django.shortcuts import render


def pwa_app(request):
    return render(request, "logistica/pwa.html")
