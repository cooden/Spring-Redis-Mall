var fForm = $("#editFlow"), wIndex;
function fillForm(data) {
    fForm.find("input").each(function () {
        $(this).val(data[$(this).attr("name")]);
    });
}

function clearForm() {
    fForm.find("input").each(function () {
        //初始化为0
        $(this).val(0);
        if ($(this).attr("name") && $(this).attr("name") == "remark") {
            $(this).val("");
        }
    });
    $(".form-group").each(function () {
        $(this).removeClass("has-error").removeClass("has-success");
    });
    $(".help-block").each(function () {
        $(this).remove();
    })
    return;
}