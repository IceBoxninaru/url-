class MethodOverrideMiddleware:
    allowed_methods = {"PATCH", "DELETE"}

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.method == "POST":
            override = request.POST.get("_method") or request.headers.get("X-HTTP-Method-Override")
            if override:
                override = override.upper()
                if override in self.allowed_methods:
                    request.method = override
                    request.META["REQUEST_METHOD"] = override
        return self.get_response(request)
