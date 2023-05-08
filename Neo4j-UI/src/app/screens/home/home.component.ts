import {Component, OnInit} from '@angular/core';
import {HttpClient, HttpHeaders} from "@angular/common/http";
import {Observable} from "rxjs";
import {MoviesService} from "../../services/app.service";
import {environment} from "../../../environments/environment";
import {Movie} from "../../models/movie";

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit{
  movies?: Movie[];
  recommendations?: Movie[];
  constructor(private productService: MoviesService, private http: HttpClient) {}
  ngOnInit(): void {
    this.getProducts();
  }
  getProducts(): void {
    this.productService.getProducts().subscribe(movies => {
      this.movies = movies;
    });
  }

  getRecommendations(): void {
    this.productService.getRecommendations().subscribe(movies => {
      this.recommendations = movies;
    });
  }
}
