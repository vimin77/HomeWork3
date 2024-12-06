package ru.topacademy.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.awt.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Symbol {
    private long id;
    private String value;
    private Color colour;
    private int type;
}

