from django.shortcuts import render

def pumps_page(request):
    return render(request, "pumps/index.html")
