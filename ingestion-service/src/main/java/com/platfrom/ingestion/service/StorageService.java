package com.platfrom.ingestion.service;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Service
public class StorageService {
    
    public String upload(MultipartFile file) throws IOException {
        String fileName = UUID.randomUUID() + "_" + file.getOriginalFilename();
        
        Path path = Paths.get("uploads/" + fileName);
        Files.createDirectories(path.getParent());
        Files.copy(file.getInputStream(), path);
        
        return "http://localhost:8080/files/" + fileName;
    }
}
