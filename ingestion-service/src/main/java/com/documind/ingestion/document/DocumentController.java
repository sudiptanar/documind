package com.documind.ingestion.document;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/documents")
@RequiredArgsConstructor
public class DocumentController {

    private final DocumentService documents;

    /** 202: the PDF is stored, indexing continues asynchronously. Poll GET /{id} or listen for the notification. */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public DocumentDto upload(@AuthenticationPrincipal Jwt jwt, @RequestPart("file") MultipartFile file) {
        return documents.upload(CurrentUser.from(jwt), file);
    }

    @GetMapping
    public List<DocumentDto> list(@AuthenticationPrincipal Jwt jwt) {
        return documents.list(CurrentUser.from(jwt));
    }

    @GetMapping("/{id}")
    public DocumentDto get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return documents.get(CurrentUser.from(jwt), id);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        documents.delete(CurrentUser.from(jwt), id);
    }
}
