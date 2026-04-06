from django.contrib import messages
from django.http import HttpResponseNotAllowed
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from tags.forms import TagForm
from tags.models import Tag


@ensure_csrf_cookie
@require_http_methods(["GET", "POST"])
def tag_list(request):
    form = TagForm()
    if request.method == "POST":
        form = TagForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "タグを追加しました。")
            return redirect("tags:list")
        messages.error(request, "タグの追加に失敗しました。")

    tags = list(Tag.objects.all())
    return render(
        request,
        "tags/list.html",
        {
            "form": form,
            "tags": tags,
        },
    )


@require_http_methods(["POST"])
def tag_detail(request, pk: int):
    tag = get_object_or_404(Tag, pk=pk)
    method = request.POST.get("_method", "POST").upper()

    if method == "PATCH":
        form = TagForm(request.POST, instance=tag)
        if form.is_valid():
            form.save()
            messages.success(request, "タグを更新しました。")
        else:
            messages.error(request, "タグの更新に失敗しました。")
        return redirect("tags:list")

    if method == "DELETE":
        tag.delete()
        messages.success(request, "タグを削除しました。")
        return redirect("tags:list")

    return HttpResponseNotAllowed(["POST"])
